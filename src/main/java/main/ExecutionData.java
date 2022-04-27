package main;

import java.time.Duration;

public record ExecutionData(Duration firstHalf, Duration secondHalf, Duration total) {

    public void showTimeInSeconds(){
        System.out.println(firstHalf.toSeconds());
        System.out.println(secondHalf.toSeconds());
        System.out.println(total.toSeconds());
    }

    public void showSpeedUp(ExecutionData other) {
        System.out.println(firstHalf.toNanos()/other.firstHalf.toNanos());
        System.out.println(secondHalf.toNanos()/other.secondHalf.toNanos());
        System.out.println(total.toNanos()/other.total.toNanos());
    }
}
