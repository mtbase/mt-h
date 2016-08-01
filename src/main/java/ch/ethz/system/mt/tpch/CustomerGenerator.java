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

import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

import static ch.ethz.system.mt.tpch.MTUtils.CURRENCY_DOMAIN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static ch.ethz.system.mt.tpch.MTUtils.PHONE_DOMAIN;
import static ch.ethz.system.mt.tpch.GenerateUtils.calculateRowCount;
import static ch.ethz.system.mt.tpch.GenerateUtils.calculateStartIndex;
import static java.util.Locale.ENGLISH;

public class CustomerGenerator
        implements TpchSchemaInterface<Customer>
{
    public static final int SCALE_BASE = 150_000;
    private static final int ACCOUNT_BALANCE_MIN = -99999;
    private static final int ACCOUNT_BALANCE_MAX = 999999;
    private static final int ADDRESS_AVERAGE_LENGTH = 25;
    private static final int COMMENT_AVERAGE_LENGTH = 73;

    private final double scaleFactor;
    private final int part;
    private final int partCount;

    private final int[][] formatIndexes;
    public int[] distDataSize;

    private final Distributions distributions;
    private final TextPool textPool;

    public CustomerGenerator(double scaleFactor, int part, int partCount, int[] distBlockSize, int[][] formatIndexes)
    {
        this(scaleFactor, part, partCount, distBlockSize, formatIndexes, Distributions.getDefaultDistributions(), TextPool.getDefaultTestPool());
    }

    public CustomerGenerator(double scaleFactor, int part, int partCount, int[] distBlockSize, int [][] formatIndexes, Distributions distributions, TextPool textPool)
    {
        checkArgument(scaleFactor > 0, "scaleFactor must be greater than 0");
        checkArgument(part >= 1, "part must be at least 1");
        checkArgument(part <= partCount, "part must be less than or equal to part count");

        this.scaleFactor = scaleFactor;
        this.part = part;
        this.partCount = partCount;
        this.distDataSize = distBlockSize;
        this.formatIndexes = formatIndexes;
        this.distributions = checkNotNull(distributions, "distributions is null");
        this.textPool = checkNotNull(textPool, "textPool is null");
    }

    @Override
    public Iterator<Customer> iterator()
    {
        return new CustomerGeneratorIterator(
                distributions,
                textPool,
                distDataSize,
                formatIndexes,
                calculateStartIndex(SCALE_BASE, scaleFactor, part, partCount),
                calculateRowCount(SCALE_BASE, scaleFactor, part, partCount));
    }

    private static class CustomerGeneratorIterator
            extends AbstractIterator<Customer>
    {
        private final RandomAlphaNumeric addressRandom = new RandomAlphaNumeric(881155353, ADDRESS_AVERAGE_LENGTH);
        private final RandomBoundedInt nationKeyRandom;
        private final RandomPhoneNumber phoneRandom = new RandomPhoneNumber(1521138112);
        private final RandomCurrencyValue accountBalanceRandom;
        private final RandomString marketSegmentRandom;
        private final RandomText commentRandom;

        private final long startIndex;
        private final long rowCount;

        private long rowsProduced = 0;
        private long counter;
        private int[] dataBlock;
        private int tenantIndex;
        private int[][] formatIndexes;

        private CustomerGeneratorIterator(Distributions distributions, TextPool textPool, int[] dataBlock, int[][] formatIndexes, long startIndex, long rowCount)
        {
            this.startIndex = startIndex;
            this.rowCount = rowCount;
            this.dataBlock = dataBlock;
            nationKeyRandom = new RandomBoundedInt(1489529863, 0, distributions.getNations().size() - 1);
            accountBalanceRandom = new RandomCurrencyValue(298370230, ACCOUNT_BALANCE_MIN, ACCOUNT_BALANCE_MAX);
            marketSegmentRandom = new RandomString(1140279430, distributions.getMarketSegments());
            commentRandom = new RandomText(1335826707, textPool, COMMENT_AVERAGE_LENGTH);

            this.tenantIndex = 0;
            counter = startIndex;
            while (counter > dataBlock[tenantIndex]) {
                counter -= dataBlock[tenantIndex];
                tenantIndex++;
            }
            this.formatIndexes = formatIndexes;
            phoneRandom.setFormatIndex(this.formatIndexes[tenantIndex][PHONE_DOMAIN]);
            accountBalanceRandom.setFormatIndex(this.formatIndexes[tenantIndex][CURRENCY_DOMAIN]);

            addressRandom.advanceRows(startIndex);
            nationKeyRandom.advanceRows(startIndex);
            phoneRandom.advanceRows(startIndex);
            accountBalanceRandom.advanceRows(startIndex);
            marketSegmentRandom.advanceRows(startIndex);
            commentRandom.advanceRows(startIndex);
        }

        @Override
        protected Customer computeNext()
        {
            if (rowsProduced >= rowCount) {
                return endOfData();
            }

            //if the counter reach the size of that tenant, move to the next index and get the size of that tenant
            if (counter == dataBlock[tenantIndex]) {
                tenantIndex++;
                counter = 0;
                phoneRandom.setFormatIndex(formatIndexes[tenantIndex][PHONE_DOMAIN]);
                accountBalanceRandom.setFormatIndex(formatIndexes[tenantIndex][CURRENCY_DOMAIN]);
            }

            Customer customer = makeCustomer(tenantIndex+1, startIndex+rowsProduced+1, counter+1);

            addressRandom.rowFinished();
            nationKeyRandom.rowFinished();
            phoneRandom.rowFinished();
            accountBalanceRandom.rowFinished();
            marketSegmentRandom.rowFinished();
            commentRandom.rowFinished();

            counter++;
            rowsProduced++;

            return customer;
        }

        private Customer makeCustomer(int tenantKey, long rowNumber, long customerKey)
        {
            long nationKey = nationKeyRandom.nextValue();

            return new Customer(
                    tenantKey,
                    rowNumber,
                    customerKey,
                    String.format(ENGLISH, "Customer#%09d", customerKey),
                    addressRandom.nextValue(),
                    nationKey,
                    phoneRandom.nextValue(nationKey),
                    accountBalanceRandom.nextValue(),
                    marketSegmentRandom.nextValue(),
                    commentRandom.nextValue());
        }
    }
}
