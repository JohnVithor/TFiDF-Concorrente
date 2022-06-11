package jv.jcstress;

import jv.utils.MyBuffer;
import org.openjdk.jcstress.annotations.*;

@State
public class BufferState extends MyBuffer<String> {
    public BufferState() {
        super(2);
    }
}