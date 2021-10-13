package be.uantwerpen.adrem.disteclat;
import org.junit.Test;


public class DistEclatSparkDriverTest {

    @Test
    public void runMiner()  {
        DistEclatSparkDriver.main(new String[]{"chess.dat.txt", "2000", "1"});
    }
}
