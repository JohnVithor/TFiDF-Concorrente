package jv.stresstest;

import jv.utils.MyBuffer;
import org.openjdk.jcstress.annotations.State;

@State
public class CharBufferState extends MyBuffer<Character> {
    public CharBufferState() {
        super(2);
    }
}
