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
import static ch.ethz.system.mt.tpch.TpchColumnTypes.DOUBLE;
import static ch.ethz.system.mt.tpch.TpchColumnTypes.varchar;

public enum CustomerColumn
        implements TpchColumn<Customer>
{
    CUSTOMER_KEY("custkey", IDENTIFIER)
            {
                public long getIdentifier(Customer customer)
                {
                    return customer.getCustomerKey();
                }
            },

    NAME("name", varchar(25))
            {
                public String getString(Customer customer)
                {
                    return customer.getName();
                }
            },

    ADDRESS("address", varchar(40))
            {
                public String getString(Customer customer)
                {
                    return customer.getAddress();
                }
            },

    NATION_KEY("nationkey", IDENTIFIER)
            {
                public long getIdentifier(Customer customer)
                {
                    return customer.getNationKey();
                }
            },

    PHONE("phone", varchar(15))
            {
                public String getString(Customer customer)
                {
                    return customer.getPhone();
                }
            },

    ACCOUNT_BALANCE("acctbal", DOUBLE)
            {
                public double getDouble(Customer customer)
                {
                    return customer.getAccountBalance();
                }

                public long getIdentifier(Customer customer)
                {
                    return customer.getAccountBalanceInCents();
                }
            },

    MARKET_SEGMENT("mktsegment", varchar(10))
            {
                public String getString(Customer customer)
                {
                    return customer.getMarketSegment();
                }
            },

    COMMENT("comment", varchar(117))
            {
                public String getString(Customer customer)
                {
                    return customer.getComment();
                }
            };

    private final String columnName;
    private final TpchColumnType type;

    CustomerColumn(String columnName, TpchColumnType type)
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
    public double getDouble(Customer customer)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getIdentifier(Customer customer)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInteger(Customer customer)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(Customer customer)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDate(Customer entity)
    {
        throw new UnsupportedOperationException();
    }
}