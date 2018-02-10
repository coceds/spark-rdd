package rdd;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Resources;
import org.apache.commons.net.util.SubnetUtils;

import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CodeToIp implements Serializable {

    static final String IP_FILE = "data.csv";

    private final TreeMap<Long, Entry> map = new TreeMap<>();

    void addNetwork(long startIp, long endIp, String geoId) {
        map.put(startIp, new Entry(endIp, geoId));
    }

    public String getGeoId(String ip) {
        long parsed = ipToLong(ip);
        Map.Entry<Long, Entry> entry = map.floorEntry(parsed);
        if (entry != null && parsed <= entry.getValue().end) {
            return new String(entry.getValue().geoId);
        } else {
            return null;
        }
    }

    public static CodeToIp init() {
        CodeToIp map = new CodeToIp();
        try {
            URL url = Resources.getResource(IP_FILE);
            List<String> lines = Resources.readLines(url, Charsets.UTF_8);
            for (String line : lines) {
                String[] parts = line.split(",");
                if (parts.length >= 6) {
                    SubnetUtils utils = new SubnetUtils(parts[0].trim());
                    long begin = ipToLong(utils.getInfo().getLowAddress()) - 1;
                    long end = ipToLong(utils.getInfo().getHighAddress()) + 1;
                    map.addNetwork(begin, end, parts[1].trim());
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Throwables.propagate(e);
        }
        return map;
    }

    public static class Entry implements Serializable {

        private final long end;
        private final byte[] geoId;

        Entry(long end, String geoId) {
            this.end = end;
            this.geoId = geoId.getBytes();
        }
    }

    static long ipToLong(String ipAddress) {
        String[] ipAddressInArray = ipAddress.split("\\.");
        long result = 0;
        for (int i = 0; i < ipAddressInArray.length; i++) {

            int power = 3 - i;
            int ip = Integer.parseInt(ipAddressInArray[i]);
            result += ip * Math.pow(256, power);
        }
        return result;
    }
}
