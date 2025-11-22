package in.sunilsarolkar.javacore;

import java.util.Comparator;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class TeeingExample {
    public static void main(String[] args) {
        List<Integer> list=List.of(1,2,3,4,5,6,7);
        double average=list.stream().collect(
            Collectors.teeing(Collectors.summingInt(i->i), Collectors.counting(), (sum,cnt)->sum/(double)cnt));
        System.out.println("Average "+average);

        Map<IntSummaryStatistics,Object> summary =list.stream().collect(Collectors.teeing(Collectors.summarizingInt(Integer::intValue),Collectors.toList(),(stats,lst)->Map.of(stats,lst)));
        System.out.println(summary);

        var isEven=list.stream().collect(Collectors.toMap((o->o.intValue()), o->o%2==0?"even":"odd"));
        System.out.println(isEven);


        Map<String,Object> minMax=list.stream().collect(Collectors.teeing(Collectors.maxBy(Comparator.naturalOrder()), Collectors.minBy(Comparator.naturalOrder()), (max,min)->Map.of("max",max,"min",min)));
        System.out.println(minMax);
    }
}
