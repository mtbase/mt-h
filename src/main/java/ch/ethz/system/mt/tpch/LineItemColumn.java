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

public enum LineItemColumn
        implements TpchColumn<LineItem>
{
    @SuppressWarnings("SpellCheckingInspection")
    ORDER_KEY("orderkey", IDENTIFIER)
            {
                public long getIdentifier(LineItem lineItem)
                {
                    return lineItem.getOrderKey();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    PART_KEY("partkey", IDENTIFIER)
            {
                public long getIdentifier(LineItem lineItem)
                {
                    return lineItem.getPartKey();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SUPPLIER_KEY("suppkey", IDENTIFIER)
            {
                public long getIdentifier(LineItem lineItem)
                {
                    return lineItem.getSupplierKey();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    LINE_NUMBER("linenumber", INTEGER)
            {
                public int getInteger(LineItem lineItem)
                {
                    return lineItem.getLineNumber();
                }
            },

    QUANTITY("quantity", DOUBLE)
            {
                public double getDouble(LineItem lineItem)
                {
                    return lineItem.getQuantity();
                }

                public long getIdentifier(LineItem lineItem)
                {
                    return lineItem.getQuantity() * 100;
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    EXTENDED_PRICE("extendedprice", DOUBLE)
            {
                public double getDouble(LineItem lineItem)
                {
                    return lineItem.getExtendedPrice();
                }

                public long getIdentifier(LineItem lineItem)
                {
                    return lineItem.getExtendedPriceInCents();
                }
            },

    DISCOUNT("discount", DOUBLE)
            {
                public double getDouble(LineItem lineItem)
                {
                    return lineItem.getDiscount();
                }

                public long getIdentifier(LineItem lineItem)
                {
                    return lineItem.getDiscountPercent();
                }
            },

    TAX("tax", DOUBLE)
            {
                public double getDouble(LineItem lineItem)
                {
                    return lineItem.getTax();
                }

                public long getIdentifier(LineItem lineItem)
                {
                    return lineItem.getTaxPercent();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    RETURN_FLAG("returnflag", varchar(1))
            {
                public String getString(LineItem lineItem)
                {
                    return lineItem.getReturnFlag();
                }
            },

    STATUS("linestatus", varchar(1))
            {
                public String getString(LineItem lineItem)
                {
                    return lineItem.getStatus();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_DATE("shipdate", DATE)
            {
                public String getString(LineItem lineItem)
                {
                    return formatDate(getDate(lineItem));
                }

                public int getDate(LineItem lineItem)
                {
                    return lineItem.getShipDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    COMMIT_DATE("commitdate", DATE)
            {
                public String getString(LineItem lineItem)
                {
                    return formatDate(getDate(lineItem));
                }

                public int getDate(LineItem lineItem)
                {
                    return lineItem.getCommitDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    RECEIPT_DATE("receiptdate", DATE)
            {
                public String getString(LineItem lineItem)
                {
                    return formatDate(getDate(lineItem));
                }

                @Override
                public int getDate(LineItem lineItem)
                {
                    return lineItem.getReceiptDate();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_INSTRUCTIONS("shipinstruct", varchar(25))
            {
                public String getString(LineItem lineItem)
                {
                    return lineItem.getShipInstructions();
                }
            },

    @SuppressWarnings("SpellCheckingInspection")
    SHIP_MODE("shipmode", varchar(10))
            {
                public String getString(LineItem lineItem)
                {
                    return lineItem.getShipMode();
                }
            },

    COMMENT("comment", varchar(44))
            {
                public String getString(LineItem lineItem)
                {
                    return lineItem.getComment();
                }
            };


    private final String columnName;
    private final TpchColumnType type;

    LineItemColumn(String columnName, TpchColumnType type)
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
    public double getDouble(LineItem lineItem)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getIdentifier(LineItem lineItem)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInteger(LineItem lineItem)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(LineItem lineItem)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDate(LineItem entity)
    {
        throw new UnsupportedOperationException();
    }
}