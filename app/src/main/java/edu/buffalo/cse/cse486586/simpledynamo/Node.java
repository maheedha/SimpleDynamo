package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by MaHi on 5/10/2017.
 */

public class Node {
    public String myPort,succ,pred;
    List<String> sortedHash = new ArrayList<String>();
    public Node() {
        this.myPort = myPort;
        this.succ = succ;
        this.pred=pred;
    }
    public void nodeJoin(String myPort,List activePort)
    {

        Collections.sort(activePort);
        sortedHash.clear();
        sortedHash.addAll(activePort);

        if(myPort.equals(sortedHash.get(0)))
        {
            succ = (String)sortedHash.get(1);
            pred = (String) sortedHash.get(sortedHash.size()-1);

        }
        if(myPort.equals(sortedHash.get(sortedHash.size()-1)))
        {
            succ = (String)sortedHash.get(0);
            pred = (String) sortedHash.get(sortedHash.size()-2);
        }
        for (int i=1;i<sortedHash.size()-2;i++)
        {
            if(myPort.equals(sortedHash.get(i)))
            {
                succ= (String)sortedHash.get(i+1);
                pred= (String)sortedHash.get(i-1);
            }
        }
    }
}
