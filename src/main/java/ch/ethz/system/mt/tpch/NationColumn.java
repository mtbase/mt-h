/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ch.ethz.system.mt.tpch;

import static ch.ethz.system.mt.tpch.TpchColumnTypes.IDENTIFIER;
import static ch.ethz.system.mt.tpch.TpchColumnTypes.varchar;

public enum NationColumn
        implements TpchColumn<Nation>
{
    @SuppressWarnings("SpellCheckingInspection")
    NATION_KEY("nationkey", IDENTIFIER)
            {
                public long getIdentifier(Nation nation)
                {
                    return nation.getNationKey();
                }
            },

    NAME("name", varchar(25))
            {
                public String getString(Nation nation)
                {
                    return nation.getName();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    REGION_KEY("regionkey", IDENTIFIER)
            {
                public long getIdentifier(Nation nation)
                {
                    return nation.getRegionKey();
                }
            },

    COMMENT("comment", varchar(152))
            {
                public String getString(Nation nation)
                {
                    return nation.getComment();
                }
            };

    private final String columnName;
    private final TpchColumnType type;

    NationColumn(String columnName, TpchColumnType type)
    {
        this.columnName = columnName;
        this.type = type;
    }

    @Override
    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public TpchColumnType getType()
    {
        return type;
    }

    @Override
    public double getDouble(Nation nation)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getIdentifier(Nation nation)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInteger(Nation nation)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(Nation nation)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDate(Nation entity)
    {
        throw new UnsupportedOperationException();
    }
}
