package dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests;

import dk.ku.di.dms.vms.flightScheduler.test.Util.Util;
import dk.ku.di.dms.vms.flightScheduler.test.Util.VmsProcess;

import java.util.List;

public class CustomerCrash
{
    public static boolean Run()
    {
        List<String> components = List.of("customer", "booking", "flight", "payment");
        VmsProcess.KillComponents(components);
        Util.Sleep(500);
        try {
            VmsProcess.StartComponents(components);

            long batchTimeoutWindow = Long.MAX_VALUE;
            int batchMaxTransactions = 10;
            VmsProcess.VmsProcessBuilder(
                    "proxy",
                    false,
                    List.of(String.valueOf(batchTimeoutWindow), String.valueOf(batchMaxTransactions))
            );

        } catch (Exception e) {
            System.out.println("Failure starting components");
            return false;
        }



        return true;
    }
}
