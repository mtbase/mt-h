package ch.ethz.system.mt.tpch;

import java.io.IOException;

import static ch.ethz.system.mt.tpch.MTUtils.*;

/**
 * Created by braunl on 01.07.16.
 */
public class TableFileRunnable<E extends TpchEntity> implements Runnable {

    String tableName;
    String outputDir;
    DbFormat format;
    int part;
    Iterable<E> generator;

    TableFileRunnable(String tableName, String outputDir, DbFormat format, int part, Iterable<E> generator) {
        this.tableName = tableName;
        this.outputDir = outputDir;
        this.format = format;
        this.part = part;
        this.generator = generator;
    }

    @Override
    public void run() {
        try {
            createTableFile(tableName, outputDir, format, part, generator);
        } catch (IOException e) {
            System.out.println("Exception during creation of part " + part + " of " + tableName + " table");
            e.printStackTrace();
        }
    }

}