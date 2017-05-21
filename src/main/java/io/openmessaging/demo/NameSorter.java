package io.openmessaging.demo;

import java.util.Comparator;

/**
 * Created by IceSea on 2017/5/21.
 * GitHub: https://github.com/IceSeaOnly
 */
public class NameSorter implements Comparator {
    @Override
    public int compare(Object o1, Object o2) {
        String a = (String) o1;
        String b = (String) o2;
        int A = Integer.parseInt(a.split("-")[1]);
        int B = Integer.parseInt(b.split("-")[1]);
        return A-B;
    }
}
