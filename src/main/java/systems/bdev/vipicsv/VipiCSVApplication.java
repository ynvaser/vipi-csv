package systems.bdev.vipicsv;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import systems.bdev.vipicsv.core.CsvProcessor;
import systems.bdev.vipicsv.core.MatrixCsvProcessor;

import java.io.Console;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@SpringBootApplication
@Slf4j
public class VipiCSVApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(VipiCSVApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        boolean isMatrixUseCase = args.length > 1 && "matrix".equalsIgnoreCase(args[1]);
        long interval = askForInterval();
        File jarLocation = getJarLocation(args[0]);
        Path inputFolderPath = createFolderIfNeeded(jarLocation, "\\input");
        Path outputFolderPath = createFolderIfNeeded(jarLocation, "\\output");
        File[] inputFilesArray = inputFolderPath.toFile().listFiles();

        Path cameraActivityIntervalPath = isMatrixUseCase ? createFolderIfNeeded(jarLocation, "\\camerainfo") : null;
        File cameraActivityFolder = isMatrixUseCase ? cameraActivityIntervalPath.toFile() : null;
        File cameraActivityFile = isMatrixUseCase ? new File(cameraActivityFolder, "cameras.txt") : null;
        if (isMatrixUseCase && !cameraActivityFile.exists()) {
            throw new IllegalStateException("Please provide the following file: .../camerainfo/cameras.txt");
        }
        if (inputFilesArray != null && inputFilesArray.length != 0) {
            log.info("Files present in \"{}\", processing...", inputFolderPath);
            for (File file : inputFilesArray) {
                CsvProcessor csvProcessor = isMatrixUseCase ? new MatrixCsvProcessor(interval, file, cameraActivityFile) : new CsvProcessor(interval, file);
                String outputFileName = outputFolderPath.toString() + "\\out-" + file.getName();
                try (FileWriter fileWriter = new FileWriter(outputFileName)) {
                    fileWriter.write(csvProcessor.getProcessedFileContents());
                }
                log.info("Output file {} created!", outputFileName);
            }
        } else {
            log.error("No files present in input directory: {}", inputFolderPath);
        }
        log.info("Application finished, please check the output folder for results.");
    }

    private long askForInterval() {
        String value = "";
        try {
            Console console = System.console();
            console.writer().println("------------------------------------\nPlease enter the desired interval period in minutes: ");
            value = console.readLine();
            return Long.parseLong(value);
        } catch (Exception e) {
            log.error("Couldn't parse number: {}", value);
            throw e;
        }
    }

    private Path createFolderIfNeeded(File jarLocation, String folderName) throws IOException {
        File inputFolder = new File(jarLocation.getPath() + folderName);
        Path path = inputFolder.toPath();
        if (!inputFolder.exists()) {
            log.info("Folder \"{}\" doesn't exist, creating...", path);
            Files.createDirectories(path);
        } else {
            log.info("Folder \"{}\" exists, skipping creation...", path);
        }
        return path;
    }

    private File getJarLocation(String arg) {
        return new File(arg);
    }
}
