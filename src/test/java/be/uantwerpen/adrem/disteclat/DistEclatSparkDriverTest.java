package be.uantwerpen.adrem.disteclat;
import org.junit.Test;


public class DistEclatSparkDriverTest {

    @Test
    protected void runMiner(String inputFile) throws Exception {
        DistEclatSparkDriver.main(new String[]{"chess.dat.txt", "2000", "1"});
    }
}
