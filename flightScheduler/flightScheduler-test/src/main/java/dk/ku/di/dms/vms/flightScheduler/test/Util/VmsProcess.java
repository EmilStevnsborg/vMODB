package dk.ku.di.dms.vms.flightScheduler.test.Util;

import java.io.File;

public class VmsProcess
{
    private static ProcessBuilder CreateProcessBuilder(String jarPath, boolean recoverEnabled)
    {
        String javaBin = STR."\{System.getProperty("java.home")}\{File.separator}bin\{File.separator}java";

        var processBuilder = new ProcessBuilder(
                javaBin,
                "--enable-preview",
                "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens", "java.base/jdk.internal.util=ALL-UNNAMED",
                "-jar",
                jarPath,
                STR."--recoveryEnabled=\{recoverEnabled}"
        );

        processBuilder.directory(new File("../flightScheduler-customer"));

        processBuilder.inheritIO();
        return processBuilder;
    }

    public static void KillCurrentVmsProcess(String vmsIdentifier)
    {
        var vms = STR."flightScheduler-\{vmsIdentifier}";
        ProcessHandle.allProcesses()
                .filter(ph -> ph.info().commandLine().isPresent())
                .filter(ph -> ph.info().commandLine().get().contains(vms))
                .forEach(ph -> {
                    ph.destroy();
                    while (ph.isAlive()) {}
                });

        System.out.println(STR."\{vmsIdentifier} killed");
    }
    public static ProcessBuilder VmsProcessBuilder(String vmsIdentifier, boolean recoverEnabled)
    {
        var jarPath = STR."../flightScheduler-\{vmsIdentifier}/target/flightScheduler-\{vmsIdentifier}-1.0-SNAPSHOT-jar-with-dependencies.jar";
        var processBuilder = CreateProcessBuilder(jarPath, recoverEnabled);

        return processBuilder;
    }
}
