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

import static ch.ethz.system.mt.tpch.MTUtils.BASE_PHONE_FORMAT;
import static ch.ethz.system.mt.tpch.MTUtils.PHONE_DOMAIN_SIZE;
import static ch.ethz.system.mt.tpch.MTUtils.PHONE_DOMAIN_VALUES;

import static java.util.Locale.ENGLISH;

public class RandomPhoneNumber
        extends AbstractRandomInt
{
    // limited by country codes in phone numbers
    private static final int NATIONS_MAX = 90;
    private String phoneFormatString = BASE_PHONE_FORMAT;

    public RandomPhoneNumber(long seed)
    {
        this(seed, 1);
    }

    public RandomPhoneNumber(long seed, int expectedRowCount)
    {
        super(seed, 3 * expectedRowCount);
    }

    public void setFormatIndex(int formatIndex) {
        if (formatIndex >= PHONE_DOMAIN_SIZE)
            throw new RuntimeException("Phone format index must be below" + PHONE_DOMAIN_SIZE + "! Current value: " + formatIndex);
        phoneFormatString = PHONE_DOMAIN_VALUES[formatIndex] + BASE_PHONE_FORMAT;
    }

    public String nextValue(long nationKey)
    {
        return String.format(ENGLISH,
                phoneFormatString,
                (10 + (nationKey % NATIONS_MAX)),
                nextInt(100, 999),
                nextInt(100, 999),
                nextInt(1000, 9999));
    }
}
