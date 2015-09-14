package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by Pradeep on 4/1/15.
 */
public class Message implements Serializable {

    String flag;
    int nodeID;
    String key;
    String value;
    String selection;
    ArrayList<Integer> list = new ArrayList<>();
    HashMap<String,String> map = new HashMap<String,String>();

    Message(String t, ArrayList l){
        this.flag = t;
        this.list = l;
    }

    Message(String t, int i){
        this.flag = t;
        this.nodeID = i;
    }

    Message(String t, String k, String v){
        this.flag = t;
        this.key = k;
        this.value = v;
    }

    Message(String t,String q){
        this.flag = t;
        this.selection = q;
    }

    Message(String v){
        this.value = v;
    }

    Message(HashMap  h){
        this.map = h;
    }
}
