package dk.ku.di.dms.vms.modb.common.logging;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;

public final class LoggingHandlerBuilder {

    public static ILoggingHandler build(String identifier) {
//        String fileName = identifier + "_" + new Date().getTime() +".llog";
        String fileName = identifier + ".llog";
//        String userHome = ConfigUtils.getUserHome();
        String currentDir = System.getProperty("user.dir");
        String basePath = currentDir + "/logs";
        File theDir = new File(basePath);

        System.out.println("\n############# LoggingHandlerBuilder ###############\n");
        System.out.println(STR."modb.common.logging.LoggingHandlerBuilder.build path: \{theDir.getAbsolutePath()}");

        assert theDir.exists() || theDir.mkdirs();
        String filePath = basePath + "/" + fileName;
        Path path = Paths.get(filePath);
        FileChannel fileChannel;
        try {
            fileChannel = FileChannel.open(path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String loggingType = System.getProperty("logging_type");
        ILoggingHandler handler;
        try {
            if (loggingType == null || loggingType.isEmpty() ||loggingType.contentEquals("thesisV0")) {
                System.out.println("thesis v0 logging type");
                handler = new ThesisLoggingHandlerV0(fileChannel, fileName);
            }
            else if(loggingType.contentEquals("default")){
                System.out.println("default logging type");
                handler = new DefaultLoggingHandler(fileChannel, fileName);
            }
            else {
                System.out.println("compressed logging type");
                handler = new CompressedLoggingHandler(fileChannel, fileName);
            }
        } catch (NoClassDefFoundError | Exception e) {
            System.out.println("Failed to load compressed logging handler, setting the default. Error:\n"+e);
            handler = new DefaultLoggingHandler(fileChannel, fileName);
        }
        return handler;
    }

}
