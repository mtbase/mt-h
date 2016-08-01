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

import static ch.ethz.system.mt.tpch.GenerateUtils.formatDate;
import static ch.ethz.system.mt.tpch.TpchColumnTypes.IDENTIFIER;
import static ch.ethz.system.mt.tpch.TpchColumnTypes.DATE;
import static ch.ethz.system.mt.tpch.TpchColumnTypes.DOUBLE;
import static ch.ethz.system.mt.tpch.TpchColumnTypes.INTEGER;
import static ch.ethz.system.mt.tpch.TpchColumnTypes.varchar;

public enum OrderColumn
        implements TpchColumn<Order>
{
    @SuppressWarnings("SpellCheckingInspection")
    ORDER_KEY("orderkey", IDENTIFIER)
            {
                public long getIdentifier(Order order)
                {
                    return order.getOrderKey();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    CUSTOMER_KEY("custkey", IDENTIFIER)
            {
                public long getIdentifier(Order order)
                {
                    return order.getCustomerKey();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    ORDER_STATUS("orderstatus", varchar(1))
            {
                public String getString(Order order)
                {
                    return String.valueOf(order.getOrderStatus());
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    TOTAL_PRICE("totalprice", DOUBLE)
            {
                public double getDouble(Order order)
                {
                    return order.getTotalPrice();
                }

                public long getIdentifier(Order order)
                {
                    return order.getTotalPriceInCents();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    ORDER_DATE("orderdate", DATE)
            {
                @Override
                public String getString(Order order)
                {
                    return formatDate(getDate(order));
                }

                public int getDate(Order order)
                {
                    return order.getOrderDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    ORDER_PRIORITY("orderpriority", varchar(15))
            {
                public String getString(Order order)
                {
                    return order.getOrderPriority();
                }
            },

    CLERK("clerk", varchar(15))
            {
                public String getString(Order order)
                {
                    return order.getClerk();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_PRIORITY("shippriority", INTEGER)
            {
                public int getInteger(Order order)
                {
                    return order.getShipPriority();
                }
            },

    COMMENT("comment", varchar(79))
            {
                public String getString(Order order)
                {
                    return order.getComment();
                }
            };

    private final String columnName;
    private final TpchColumnType type;

    OrderColumn(String columnName, TpchColumnType type)
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
    public double getDouble(Order order)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getIdentifier(Order order)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInteger(Order order)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(Order order)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDate(Order entity)
    {
        throw new UnsupportedOperationException();
    }
}