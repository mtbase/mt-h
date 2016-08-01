package ch.ethz.system.mt.tpch;

import java.util.Iterator;

public interface TpchSchemaInterface<T> extends Iterable<T> {

    @Override
    Iterator<T> iterator();
}
