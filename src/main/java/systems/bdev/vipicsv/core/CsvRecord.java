package systems.bdev.vipicsv.core;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class CsvRecord implements Comparable<CsvRecord> {
    private Long cameraNumber;
    private LocalDateTime dateTime;
    private String speciesName;
    private String resultColumn = "";

    @Override
    public int compareTo(CsvRecord o) {
        return dateTime.compareTo(o.dateTime);
    }
}
