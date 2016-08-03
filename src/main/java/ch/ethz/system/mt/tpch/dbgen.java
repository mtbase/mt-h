package ch.ethz.system.mt.tpch;

import org.apache.commons.cli.*;
import sun.misc.MessageUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.DecimalFormat;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static ch.ethz.system.mt.tpch.GenerateUtils.calculateRowCount;
import static ch.ethz.system.mt.tpch.MTUtils.*;

/**
 * Entry point to generation database data for Multi-tenant version TPC-H benchmark
 */
public class dbgen {

    public static final String DEFAULT_OUTPUT_DIRECTORY = "output";

    private static final DecimalFormat currencyFormat = new DecimalFormat("######.####");

    private static void storeTenantInfo(final String outputDir, final int[] custShares, final int[] orderShares)
        throws IOException {
        // tenant info
        Writer writer = new FileWriter(outputDir + "/tenant_info.txt");
        writer.write("#Number of customers, orders, and (approximately) lineitems for each tenant\n");
        writer.write("#--------------------------------------------------------------------------\n\n");
        writer.write("#tenant-id|customers|orders|lineitems\n");
        for (int tid = 0; tid < custShares.length; tid++) {
            writer.write(String.format("%d|%d|%d|%d",
                    tid+1, custShares[tid], orderShares[tid], 4*orderShares[tid]));
            writer.write('\n');
        }
        writer.close();
    }

    private static void createTenantTableFiles(final String outputDir, final int[][] formatIndexes,
                                               final DbFormat dbFormat) throws IOException {
        boolean finalSeparator = dbFormat == DbFormat.ORACLE ? true: false;
        System.out.print("Generating data for meta tables");

        // tenant table
        Writer writer = new FileWriter(outputDir + "/tenant.tbl");
        for (int tid = 0; tid < formatIndexes.length; tid++) {
            writer.write(String.format("%d|%s|%d|%d%s",
                    tid+1, "Tenant" + (tid+1),
                    formatIndexes[tid][CURRENCY_DOMAIN], formatIndexes[tid][PHONE_DOMAIN],
                    finalSeparator?"|":""));
            writer.write('\n');
        }
        writer.close();

        // currency table
        writer = new FileWriter(outputDir + "/currency.tbl");
        for (int ckey = 0; ckey < CURRENCY_DOMAIN_SIZE; ckey++) {
            writer.write(String.format("%d|%s|%s%s",
                    ckey,
                    currencyFormat.format(1.0/ CURRENCY_DOMAIN_VALUES[ckey]),
                    currencyFormat.format(CURRENCY_DOMAIN_VALUES[ckey]),
                    finalSeparator?"|":""));
            writer.write('\n');
        }
        writer.close();

        // phone table
        writer = new FileWriter(outputDir + "/phone.tbl");
        for (int pkey = 0; pkey < PHONE_DOMAIN_SIZE; pkey++) {
            writer.write(String.format("%d|%s%s",
                    pkey, PHONE_DOMAIN_VALUES[pkey],
                    finalSeparator?"|":""));
            writer.write('\n');
        }
        writer.close();

        System.out.println("...done");
    }

    public static void main(String args[]) {

        // Parse input

        double scaleFactor = 1; //set default value for Scale Factor
        int numberOfParts = 1;  // not to be confused with the part and partsupp tables!
        int tenant = 1; //set default value for tenant number
        String outputDir = DEFAULT_OUTPUT_DIRECTORY;
        int[] custShares     = new int[tenant];
        int[] orderShares    = new int[tenant];
        int customerRowCount, orderRowCount;
        String disMode = "uniform"; //set default value for distribution mode
        DbFormat format = DbFormat.ORACLE;

        Options options = new Options();
        options.addOption("h", false, "-- display this message");
        options.addOption("s", true,  "-- set scale factor (SF) (default: 1)");
        options.addOption("t", true,  "-- set number of tenants (default: 1)");
        options.addOption("m", true,  "-- set distribution mode (uniform=default, zipf");
        options.addOption("o", true,  "-- set the output directory (default: 'output'");
        options.addOption("f", true,  "-- set the output files format <oracle (with separator at end of each line)|"
                + "postgres (no separator at line end)> (default: 'oracle')");
        options.addOption("p", true,  "-- set the number of parts the files should be divided into (default: 1)");

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse( options, args);
            if (cmd.hasOption("h")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("Main", options);
                System.exit(0);
            }
            if (cmd.hasOption("s")) {
                scaleFactor = Double.parseDouble(cmd.getOptionValue("s"));
            }
            if (cmd.hasOption("t")) {
                tenant = Integer.parseInt(cmd.getOptionValue("t"));
            }
            if (cmd.hasOption("m")) {
                String mode = cmd.getOptionValue("m");
                if (mode != null && !String.valueOf(mode).equals("uniform") && !String.valueOf(mode).equals("zipf") ) {
                    System.out.println("Distribution mode cannot be recognise. Try <uniform> or <zipf>");
                    System.exit(0);
                } else {
                    disMode = mode;
                }
            }
            if (cmd.hasOption("o")) {
                outputDir = cmd.getOptionValue("o");
            }
            if (cmd.hasOption("f")) {
                format = DbFormat.valueOf(cmd.getOptionValue('f').toUpperCase());
            }
            if (cmd.hasOption("p")) {
                numberOfParts = Integer.parseInt(cmd.getOptionValue("p"));
            }
        } catch (ParseException e) {
            System.out.println("### DB Generation Failed for the following reason:");
            e.printStackTrace();
            return;
        }

        // print parsed arguments

        System.out.println("### DB Generate Start with the following parameters: ###");
        System.out.println("scaling factor: " + scaleFactor);
        System.out.println("number of tenants: " + tenant);
        System.out.println("mode: " + disMode);
        System.out.println("outpt directory: " + outputDir);
        System.out.println("database format: " + format.toString());
        System.out.println("number of parts: " + numberOfParts);
        System.out.println("########################################################");

        // create output directory if non-existent
        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists())
            outputDirFile.mkdirs();

        // calculate total row counts
        customerRowCount     = (int) calculateRowCount(CustomerGenerator.SCALE_BASE, scaleFactor, 1, 1);
        orderRowCount        = (int) calculateRowCount(OrderGenerator.SCALE_BASE, scaleFactor, 1, 1);
        //The row count of lineitem depends on order row count and cannot be pre-calculated

        // calculate tenant shares
        switch(disMode) {
            case "uniform":
                custShares     = uniformDataDist(tenant, customerRowCount);
                orderShares    = uniformDataDist(tenant, orderRowCount);
                break;
            case "zipf":
                custShares     = zipfDataDist(tenant, customerRowCount);
                orderShares    = zipfDataDist(tenant, orderRowCount);
                break;
        }

        //Generate tenant formats
        int [][] formatIndexes = new int[tenant][NUM_DOMAINS];
        Random rnd = new Random(0);
        // First tenant should have universal format such that validation queries return the right results
        formatIndexes[0][CURRENCY_DOMAIN] = 0;
        formatIndexes[0][PHONE_DOMAIN] = 0;
        for (int tid = 1; tid < tenant; tid++) {
            formatIndexes[tid][CURRENCY_DOMAIN] = rnd.nextInt(CURRENCY_DOMAIN_SIZE);
            formatIndexes[tid][PHONE_DOMAIN] = rnd.nextInt(PHONE_DOMAIN_SIZE);
        }

        // generate table files
        try {
            storeTenantInfo(outputDir, custShares, orderShares);
            createTenantTableFiles(outputDir, formatIndexes, format);
            createTableFile("region", outputDir, format, 1, new RegionGenerator());
            createTableFile("nation", outputDir, format, 1, new NationGenerator());

            ExecutorService service = Executors.newFixedThreadPool(numberOfParts);
            for (int part = 1; part <= numberOfParts; part++) {
                service.execute(new TableFileRunnable<>("part", outputDir, format, part,
                        new PartGenerator(scaleFactor, part, numberOfParts)));
                service.execute(new TableFileRunnable<>("supplier", outputDir, format, part,
                        new SupplierGenerator(scaleFactor, part, numberOfParts)));
                service.execute(new TableFileRunnable<>("partsupp", outputDir, format, part,
                        new PartSupplierGenerator(scaleFactor, part, numberOfParts)));
                service.execute(new TableFileRunnable<>("customer", outputDir, format, part,
                        new CustomerGenerator(scaleFactor, part, numberOfParts, custShares, formatIndexes)));
                service.execute(new TableFileRunnable<>("orders", outputDir, format, part,
                        new OrderGenerator(scaleFactor, part, numberOfParts, orderShares, custShares, formatIndexes)));
                service.execute(new TableFileRunnable<>("lineitem", outputDir, format, part,
                        new LineItemGenerator(scaleFactor, part, numberOfParts, orderShares, formatIndexes)));
            }

            service.shutdown();
            while (!service.isTerminated());
            System.out.println("### DB Generate Done");
        } catch (IOException e) {
            System.out.println("### DB Generation Failed for the following reason:");
            e.printStackTrace();
        }

    }
}
