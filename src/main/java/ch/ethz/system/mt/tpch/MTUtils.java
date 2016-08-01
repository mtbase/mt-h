package ch.ethz.system.mt.tpch;

import org.apache.commons.math3.distribution.ZipfDistribution;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;


public class MTUtils {
    /**
     * CONSTANTS for convertible attribute domains
     * to get a random currency value, use RandomCurrencyValue
     * to get a random phone number, use RandomPhoneNumber
     */

    // convertible attribute domains
    public static final int CURRENCY_DOMAIN = 0;
    public static final int PHONE_DOMAIN = 1;
    public static final int NUM_DOMAINS = PHONE_DOMAIN + 1;

    // convertible attribute domain values

    public static final double[] CURRENCY_DOMAIN_VALUES = new double[] {
        /* taken from www.xe.com on 2016/06/16 */
        1.0,    /* USD=default */
        0.88753,/* EUR */
        0.70617,/* GBP */
        67.2548,/* INR */
        1.29759,/* CAD */
        1.35898,/* AUD */
        6.58648,/* CNY */
        18.9510,/* MXN */
        3.67295,/* AED */
        0.96088 /* CHF */
    };
    public static final int CURRENCY_DOMAIN_SIZE = CURRENCY_DOMAIN_VALUES.length;

    public static final String[] PHONE_DOMAIN_VALUES = new String[]{
        "",      /* default */
        "00",
        "+"
    };
    public static final String BASE_PHONE_FORMAT = "%02d-%03d-%03d-%04d";
    public static final int PHONE_DOMAIN_SIZE    = PHONE_DOMAIN_VALUES.length;

    public static int[] uniformDataDist(int numOfTenant, int rowCount) {
        int normalSize = rowCount/numOfTenant;
        int lastSize = normalSize + (rowCount % numOfTenant);
        int[] dataSize = new int[numOfTenant];

        for (int i = 0; i < numOfTenant; i++) {
            dataSize[i] = normalSize;
        }
        dataSize[numOfTenant-1] = lastSize;
        return dataSize;
    }

    public static int[] zipfDataDist(int numOfTenant, int rowCount) {
        int[] dataDist = new int[numOfTenant];
        ZipfDistribution distribution = new ZipfDistribution(numOfTenant,1);
        int sum = 0;
        int probability;
        for (int i = 1; i <= numOfTenant; i++) {
            probability = (int) Math.floor(distribution.probability(i)*rowCount);
            dataDist[i-1] = probability;
            sum = sum + probability;
        }
        dataDist[0] = dataDist[0] + (rowCount-sum);
        return dataDist;
    }

    public static <E extends TpchEntity> void createTableFile(
            final String tableName, final String outputDir, final DbFormat dbFormat, int part,
            Iterable<E> generator) throws IOException {
        boolean needFinalTab = dbFormat == DbFormat.ORACLE ? true: false;
        Writer writer = new FileWriter(outputDir + "/" + tableName + ".tbl." + part);
        System.out.println("Generating part " + part + " of " + tableName + " table");
        for (E entity : generator) {
            writer.write(entity.toLine(needFinalTab));
            writer.write('\n');
        }
        System.out.println("Finished part " + part + " of " + tableName + " table");
        writer.close();
    }

    enum DbFormat {
        ORACLE("oracle"), POSTGRES("postgres");
        private final String val;
        DbFormat(String v) {
            val = v;
        }
    }
}
