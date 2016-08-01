package ch.ethz.system.mt.tpch;

import static ch.ethz.system.mt.tpch.MTUtils.CURRENCY_DOMAIN_SIZE;
import static ch.ethz.system.mt.tpch.MTUtils.CURRENCY_DOMAIN_VALUES;

/**
 * Created by braunl on 16.06.16.
 */
public class RandomCurrencyValue extends RandomBoundedInt {

    private double currencyFactor = CURRENCY_DOMAIN_VALUES[0];

    public RandomCurrencyValue(long seed, int lowValue, int highValue)
    {
        super(seed, lowValue, highValue);
    }

    public RandomCurrencyValue(long seed, int lowValue, int highValue, int expectedRowCount)
    {
        super(seed, lowValue, highValue, expectedRowCount);
    }

    public void setFormatIndex(int formatIndex) {
        if (formatIndex >= CURRENCY_DOMAIN_SIZE)
            throw new RuntimeException("Currency format index must be below" + CURRENCY_DOMAIN_SIZE + "! Current value: " + formatIndex);
        currencyFactor = CURRENCY_DOMAIN_VALUES[formatIndex];
    }

    @Override
    protected int nextInt(int lowValue, int highValue) {
        return (int) (currencyFactor * super.nextInt(lowValue, highValue) + 0.5);
    }
}
