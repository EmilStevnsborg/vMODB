package dk.ku.di.dms.vms.flightScheduler.test;

import dk.ku.di.dms.vms.flightScheduler.test.AbortTests.AbortMidBatch;
import dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests.RecoverCoordinator;
import dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests.RecoverVms;
import dk.ku.di.dms.vms.flightScheduler.test.RecoveryTests.RecoverVmsConcurrency;
import dk.ku.di.dms.vms.flightScheduler.test.Util.ComponentProcess;

import java.net.http.HttpClient;

public final class Main
{
    private static final HttpClient client = HttpClient.newHttpClient();

    public static void main(String[] args)
    {
        try {
//            AbortMidBatch.Run(client);
//            RecoverVms.Run(client);
//            RecoverCoordinator.Run(client);
            RecoverVmsConcurrency.Run(client);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            ComponentProcess.KillVMSes();
            ComponentProcess.Kill("proxy");
        }
    }

    //

}