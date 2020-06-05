package org.apache.drill.exec.store.np.scan;

import com.mapr.db.MapRDB;
import org.apache.drill.exec.store.np.NPScanSpec;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

class TabletInfoProvider {
   private final List<NPTabletInfo> tablets;
   private NPScanSpec scanSpec;

   public TabletInfoProvider(NPScanSpec scanSpec) {
       this.scanSpec = scanSpec;

       this.tablets = getInfo();
   }

   private List<NPTabletInfo> getInfo() {
       String connectionString = this.scanSpec.getPluginConfig().getConnection();

       if (connectionString.startsWith("ojai:anicolaspp:mem")) {
           return Arrays.asList(
                   new NPTabletInfo("{}", Lists.newArrayList("192.168.0.190", "localhost2"))
//                        new NPTabletInfo("", Lists.newArrayList("localhost"))
           );
       }

       return Arrays
               .stream(MapRDB.getTable(scanSpec.getTableName()).getTabletInfos())
               .map(maprTablet -> new NPTabletInfo(
                       maprTablet.getCondition().asJsonString(),
                       Arrays.asList(maprTablet.getLocations()))
               )
               .collect(Collectors.toList());
   }

   public Collection<NPTabletInfo> getTabletInfos() {
       return tablets;
   }
}
