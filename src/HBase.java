import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class HBase {
    private static Scanner sc = new Scanner(System.in);
    private static int row = 0;

    public static void main(String[] args) throws IOException {
        {
            System.out.println("Trying to establish HBase connection...");
            Configuration hBaseConfig = HBaseConfiguration.create();
            hBaseConfig.set("hbase.zookeeper.quorum", "nqmi26");
            System.setProperty("hadoop.home.dir", "C:\\winutils\\");
            System.out.println("HBase Connection succeded...");
            HTable table = new HTable(hBaseConfig, "Member");
            System.out.println("Please select function");
            String req = sc.next();
            if (req.equals("post")) {
                table.put(put());
            } else if (req.equals("get")) {
                Get getIn = get();
                Result resName = table.get(getIn);
                Result resAge = table.get(getIn);
                Result resSex = table.get(getIn);
                byte[] valN = resName.getValue(Bytes.toBytes("people"), Bytes.toBytes("name"));
                byte[] valA = resAge.getValue(Bytes.toBytes("people"), Bytes.toBytes("age"));
                byte[] valS = resSex.getValue(Bytes.toBytes("people"), Bytes.toBytes("sex"));
                System.out.println("val:" + Bytes.toString(valN) + "\n" + Bytes.toString(valA) + "\n" + Bytes.toString(valS));

            } else if (req.equals("delete")) {
                System.out.println("Please enter the rowKey of the data you want to remove.");
                Delete del = new Delete(Bytes.toBytes(sc.next()));
                table.delete(del);
                table.close();
                System.out.println("OK");
            } else if (req.equals("put")) {
                System.out.println("Please enter the rowKey of the data you want to updata.");
                Put put = new Put(Bytes.toBytes(sc.next()));
                System.out.println("Please enter the title and new value of the data you want to updata.");
                put.add(Bytes.toBytes("people"), Bytes.toBytes(sc.next()), Bytes.toBytes(sc.next()));
                table.put(put);
                System.out.println("OK");
            }
            main(new String[0]);
        }
    }

    public static Put put() {
        row += 1;
        Put input = new Put(Bytes.toBytes(String.valueOf(row)));
        System.out.println("Please enter your name");
        input.addColumn(Bytes.toBytes("people"), Bytes.toBytes("name"), Bytes.toBytes(sc.next()));
        System.out.println("Please enter your sex");
        input.addColumn(Bytes.toBytes("people"), Bytes.toBytes("sex"), Bytes.toBytes(sc.next()));
        System.out.println("Please enter your age");
        input.addColumn(Bytes.toBytes("people"), Bytes.toBytes("age"), Bytes.toBytes(sc.next()));
        return input;
    }

    public static Get get() {
        System.out.println("Please enter the rowKey of the data you want to search for.");
        String input = sc.next();
        Get get = new Get(Bytes.toBytes((input)));
        get.addColumn(Bytes.toBytes("people"), Bytes.toBytes("name"));
        get.addColumn(Bytes.toBytes("people"), Bytes.toBytes("age"));
        get.addColumn(Bytes.toBytes("people"), Bytes.toBytes("sex"));
        return get;
    }
}