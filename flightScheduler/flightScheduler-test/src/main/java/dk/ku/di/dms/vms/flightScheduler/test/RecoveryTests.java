package dk.ku.di.dms.vms.flightScheduler.test;

import java.io.File;
import java.io.IOException;
import java.net.http.HttpClient;

public class RecoveryTests
{
    private HttpClient client;
    public RecoveryTests(HttpClient client)
    {
        this.client = client;
    }

    public void CustomerCrash() throws IOException
    {
        String javaBin = STR."\{System.getProperty("java.home")}\{File.separator}bin\{File.separator}java";
        String classpath = new File("../flightScheduler-customer/target/classes").getAbsolutePath();
        String mainClass = "dk.ku.di.dms.vms.flightScheduler.customer.Main";

        var processBuilder = new ProcessBuilder(
                javaBin, "-cp",
                classpath,
                mainClass, String.valueOf(false)
        );

        processBuilder.inheritIO();
        var initialProcess = processBuilder.start();

        //

    }
}
