package dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests;

import dk.ku.di.dms.vms.flightScheduler.test.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;

import java.util.List;

public class CustomerCrash
{
    public static boolean Run()
    {
        var batchTimeoutWindow = Integer.MAX_VALUE;
        var batchMaxTransactions = 10;

        List<String> components = List.of("customer", "booking", "flight", "payment");
        ComponentProcess.KillComponents(components);
        Util.Sleep(500);
        try {
            ComponentProcess.StartComponents(components);

            System.out.println("Starting proxy with max timeout window and 10 events for batch assignment");
            ComponentProcess.StartProxy(false, batchTimeoutWindow, batchMaxTransactions);

        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }

        // Inject data



        return true;
    }
}
