package be.uantwerpen.adrem.disteclat;
import org.junit.Test;


public class DistEclatSparkDriverTest {

    @Test
    public void runMiner()  {
        DistEclatSparkDriver.main(new String[]{"retail.dat", "10000", "1"});
    }
}
