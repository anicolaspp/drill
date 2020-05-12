package org.apache.drill.exec.store.np.scan;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.mapr.db.MapRDB;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.np.NPScanSpec;
import org.apache.drill.exec.store.np.NPStoragePlugin;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonTypeName("np-scan")
public class NPGroupScan extends AbstractGroupScan {

    private static final Logger logger = LoggerFactory.getLogger(NPGroupScan.class);

    private final NPStoragePlugin storagePlugin;
    private final NPScanSpec scanSpec;
    private final List<SchemaPath> columns;
    private final String filters;
    private final TabletInfoProvider tabletInfoProvider;

    private Map<Integer, ArrayList<NPTabletInfo>> assigments;

    public NPGroupScan(NPStoragePlugin storagePlugin,
                       NPScanSpec scanSpec) {
        super("no-user");

        this.storagePlugin = storagePlugin;
        this.scanSpec = scanSpec;
        this.columns = ALL_COLUMNS;
        this.filters = "";
        this.tabletInfoProvider = new TabletInfoProvider(this.scanSpec);
    }

    public NPGroupScan(NPGroupScan from,
                       List<SchemaPath> columns,
                       String filters) {
        super(from);

        this.storagePlugin = from.storagePlugin;
        this.scanSpec = from.scanSpec;
        this.columns = columns;
        this.filters = filters;
        this.tabletInfoProvider = from.tabletInfoProvider;
    }

    @JsonCreator
    public NPGroupScan(@JacksonInject NPStoragePlugin storagePlugin,
                       @JsonProperty("columns") List<SchemaPath> columns,
                       @JsonProperty("scanSpec") NPScanSpec scanSpec,
                       @JsonProperty("filters") String filters,
                       @JsonProperty("tabletInfoProvider") TabletInfoProvider tabletInfoProvider) {
        super("no-user");

        this.storagePlugin = storagePlugin;
        this.columns = columns;
        this.scanSpec = scanSpec;
        this.filters = filters;
        this.tabletInfoProvider = tabletInfoProvider;
    }

    @Override
    public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
        System.out.println(String.format("getSpecificScan with ID: %d invoked...", minorFragmentId));

        ArrayList<NPTabletInfo> tablets = assigments.get(minorFragmentId);

        System.out.println("tabletInfo.size() = " + tablets.size());

        return new NPSubScan(scanSpec, columns, filters, minorFragmentId, tablets);
    }

    @Override
    public boolean canPushdownProjects(List<SchemaPath> columns) {
        return true;
    }

    @Override
    public int getMaxParallelizationWidth() {
        return this.tabletInfoProvider.getTabletInfos().size();
    }

//    @Override
//    public int getMinParallelizationWidth() {
//        return this.tabletInfoProvider.getTabletInfos().size();
//    }


    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
        System.out.println(String.format("There are %d brillbits available for assigment", endpoints.size()));

        endpoints.forEach(bit -> System.out.println(bit.getControlPort()));

//        HashSet<CoordinationProtos.DrillbitEndpoint> availableBits = Sets.newHashSet(endpoints);


        assigments = new HashMap<>();

        if (tabletInfoProvider.getTabletInfos().size() == 0) {
            String preferredLocation = endpoints.get(0).getAddress();
            assigments.put(0, Lists.newArrayList(new NPTabletInfo("", Lists.newArrayList(preferredLocation))));
        } else {

            assigments = this
                    .tabletInfoProvider
                    .getTabletInfos()
                    .stream()
                    .flatMap(tabletInfo -> {
                        List<Pair<NPTabletInfo, String>> physicalLocations = tabletInfo
                                .getLocations()
                                .stream()
                                .map(location -> Pair.of(new NPTabletInfo(tabletInfo.getFilter(), Lists.newArrayList(location)), location))
                                .collect(Collectors.toList());

                        if (physicalLocations.size() == 0) {
                            return Lists
                                    .newArrayList(Pair.of(
                                            new NPTabletInfo("", Lists.newArrayList(endpoints.get(0).getAddress())),
                                            endpoints.get(0).getAddress()))
                                    .stream();
                        } else {
                            return physicalLocations.stream();
                        }

                    })
                    .map(tabletLocation -> {
                        for (int i = 0; i < endpoints.size(); i++) {
                            if (endpoints.get(i).getAddress().equals(tabletLocation.getValue())) {
                                return Pair.of(i, Lists.newArrayList(tabletLocation.getKey()));
                            }
                        }

                        return Pair.of(0, Lists.newArrayList(tabletLocation.getKey()));
                    })
                    .collect(Collectors.toMap(
                            Pair::getKey,
                            Pair::getValue,
                            (tabletInfos, tabletInfos2) -> {
                                tabletInfos.addAll(tabletInfos2);

                                return Lists.newArrayList(Sets.newHashSet(tabletInfos));
                            }));

            assigments.forEach((key, value) ->
                    System.out.println("key = " + key + " value = " + value));
        }
    }

    @Override
    public List<EndpointAffinity> getOperatorAffinity() {
        Map<String, DrillbitEndpoint> endpointsByAddress = new HashMap<>();

        storagePlugin
                .getContext()
                .getBits()
                .stream()
                .map(bit -> Pair.of(bit.getAddress(), bit))
                .forEach(pair -> {
                    if (!endpointsByAddress.containsKey(pair.getKey())) {
                        endpointsByAddress.put(pair.getKey(), pair.getValue());
                    }
                });

        Collection<NPTabletInfo> tablets = this.tabletInfoProvider.getTabletInfos();

        if (tablets.isEmpty()) {
            EndpointAffinity affinity = new EndpointAffinity(endpointsByAddress.values().stream().findAny().get(), 1);

            return Lists.newArrayList(affinity);
        }

        Map<DrillbitEndpoint, EndpointAffinity> affinityMap = new HashMap<>();

        for (NPTabletInfo tablet : tablets) {
            String address = tablet.getLocations().get(0);

            DrillbitEndpoint ep = endpointsByAddress.get(address);

            if (ep != null) {
                EndpointAffinity affinity = affinityMap.get(ep);

                if (affinity == null) {
                    affinityMap.put(ep, new EndpointAffinity(ep, 1));
                } else {
                    affinity.addAffinity(1);
                }
            }
        }

        return Lists.newArrayList(affinityMap.values());

        // As of now, considering only the first replica, though there may be
        // multiple replicas for each chunk.
//        for (Set<ServerAddress> addressList : tabletInfos) {
//            // Each replica can be on multiple machines, take the first one, which
//            // meets affinity.
//            for (ServerAddress address : addressList) {
//                DrillbitEndpoint ep = endpointMap.get(address.getHost());
//                if (ep != null) {
//                    EndpointAffinity affinity = affinityMap.get(ep);
//                    if (affinity == null) {
//                        affinityMap.put(ep, new EndpointAffinity(ep, 1));
//                    } else {
//                        affinity.addAffinity(1);
//                    }
//                    break;
//                }
//            }
//        }

//        return super.getOperatorAffinity();
    }


    @Override
    public String getDigest() {
        return toString();
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());

        return new NPGroupScan(this, columns, filters);
    }

    @Override
    public ScanStats getScanStats() {

        // If this config allows filters, then make the default
        // cost very high to force the planner to choose the version
        // with filters.
//        if (allowsFilters() && !hasFilters()) {
//            return new ScanStats(ScanStats.GroupScanProperty.ESTIMATED_TOTAL_COST,
//                    1E9, 1E112, 1E12);
//        }

        // No good estimates at all, just make up something.
        double estRowCount = 10_000;

        // NOTE this was important! if the predicates don't make the query more
        // efficient they won't get pushed down
//        if (hasFilters()) {
//            estRowCount *= filterSelectivity;
//        }

        double estColCount = Utilities.isStarQuery(columns) ? DrillScanRel.STAR_COLUMN_COST : columns.size();
        double valueCount = estRowCount * estColCount;
        double cpuCost = valueCount;
        double ioCost = valueCount;

        // Force the caller to use our costs rather than the
        // defaults (which sets IO cost to zero).
        return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT,
                estRowCount, cpuCost, ioCost);
    }

    @Override
    public GroupScan clone(List<SchemaPath> columns) {
        return new NPGroupScan(this, columns, filters);
    }

    @Override
    public String toString() {
        return new PlanStringBuilder(this)
                .field("scanSpec", scanSpec)
                .field("columns", columns)
                .field("storagePlugin", storagePlugin)
                .field("filters", filters)
                .toString();
    }

    @JsonProperty("columns")
    public List<SchemaPath> columns() {
        return columns;
    }

    @JsonProperty("scanSpec")
    public NPScanSpec getScanSpec() {
        return scanSpec;
    }

    @JsonIgnore
    public NPStoragePlugin getStoragePlugin() {
        return storagePlugin;
    }

    @JsonProperty("filters")
    public String getFilters() {
        return filters;
    }

    static class TabletInfoProvider {
        private final NPTabletInfo[] tablets;
        private NPScanSpec scanSpec;

        public TabletInfoProvider(NPScanSpec scanSpec) {
            this.scanSpec = scanSpec;

            this.tablets = getInfo();
        }

        private NPTabletInfo[] getInfo() {
            String connectionString = this.scanSpec.getPluginConfig().getConnection();

            if (connectionString.startsWith("ojai:anicolaspp:mem")) {
                return new NPTabletInfo[0];
            }

            return (NPTabletInfo[]) Arrays
                    .stream(MapRDB.getTable(scanSpec.getTableName()).getTabletInfos())
                    .map(maprTablet -> new NPTabletInfo(
                            maprTablet.getCondition().asJsonString(),
                            Arrays.asList(maprTablet.getLocations()))
                    )
                    .toArray();
        }

        public Collection<NPTabletInfo> getTabletInfos() {
            return Arrays.asList(tablets);
        }
    }
}
