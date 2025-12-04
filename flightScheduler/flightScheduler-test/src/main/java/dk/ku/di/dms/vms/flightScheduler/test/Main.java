package dk.ku.di.dms.vms.flightScheduler.test;

import dk.ku.di.dms.vms.flightScheduler.test.AbortTests.AbortConcurrently;
import dk.ku.di.dms.vms.flightScheduler.test.AbortTests.AbortMidBatch;
import dk.ku.di.dms.vms.flightScheduler.test.AbortTests.AbortStressTest;
import dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests.RecoverVms;
import dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests.RecoverVmsHeavyLoad;
import dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests.RecoverVmsStress;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;

import java.net.http.HttpClient;

public final class Main
{
    private static final HttpClient client = HttpClient.newHttpClient();

    public static void main(String[] args)
    {
        try {
//            BookingInjection.Run(client);
//            BasicAbort.Run(client);
//            LargeInjection.Run(client);
//            AbortMidBatch.Run(client);
//            AbortConcurrently.Run(client);
            RecoverVms.Run(client);
//            RecoverCoordinator.Run(client);
//            RecoverVmsStress.Run(client);
//            AbortStressTest.Run(client);
//            RecoverVmsHeavyLoad.Run(client);
//            VmsReloadPersistentData.Run(client);
//            BasicTest.Run(client);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ComponentProcess.KillVMSes();
            ComponentProcess.Kill("proxy");
        }
    }

    //

}