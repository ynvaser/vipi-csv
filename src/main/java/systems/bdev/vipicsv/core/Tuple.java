package systems.bdev.vipicsv.core;

import lombok.Data;

@Data
public class Tuple<K extends Comparable<K>, V extends Comparable<V>> implements Comparable<Tuple<K, V>> {
    public final K key;
    public final V value;

    @Override
    public int compareTo(Tuple<K, V> o) {
        int keyCompare = key.compareTo(o.getKey());
        if (keyCompare == 0) {
            return value.compareTo(o.getValue());
        }
        return keyCompare;
    }
}
