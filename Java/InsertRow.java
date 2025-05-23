package Java;
/*
 * InsertRow.java
 *
 * DBMS Implementation
 */

import java.io.*;
import java.util.Arrays;

/**
 * A class that represents a row that will be inserted in a table in a
 * relational database.
 *
 * This class contains the code used to marshall the values of the
 * individual columns to a single key-value pair.
 */
public class InsertRow {
    private Table table;           // the table in which the row will be inserted
    private Object[] columnVals;   // the column values to be inserted
    private RowOutput keyBuffer;   // buffer for the marshalled row's key
    private RowOutput valueBuffer; // buffer for the marshalled row's value
    private int[] offsets;         // offsets for header of marshalled row's value
    
    /** Constants for special offsets **/
    /** The field with this offset has a null value. */
    public static final int IS_NULL = -1;
    
    /** The field with this offset is a primary key. */
    public static final int IS_PKEY = -2;
    
    /**
     * Constructs an InsertRow object for a row containing the specified
     * values that is to be inserted in the specified table.
     *
     * @param  t  the table
     * @param  values  the column values for the row to be inserted
     */
    public InsertRow(Table table, Object[] values) {
        this.table = table;
        this.columnVals = values;
        this.keyBuffer = new RowOutput();
        this.valueBuffer = new RowOutput();
        
        // Note that we need one more offset than value,
        // so that we can store the offset of the end of the record.
        this.offsets = new int[values.length + 1];
    }
    
    /**
     * Takes the collection of values for this InsertRow
     * and marshalls them into a key/value pair.
     * 
     * (Note: We include a throws clause because this method will use 
     * methods like writeInt() that the RowOutput class inherits from 
     * DataOutputStream, and those methods could in theory throw that 
     * exception. In reality, an IOException should *not* occur in the
     * context of our RowOutput class.)
     */
    public void marshall() throws IOException {
        /* 
         * PS 3: Implement this method. 
         * 
         * Feel free to also add one or more private helper methods
         * to do some of the work (e.g., to fill in the offsets array
         * with the appropriate offsets).
         * 
         * 
         */
        Column primary = this.table.primaryKeyColumn();
        int start = (this.columnVals.length +1) *2; 
        
        for(int i=0; i<this.columnVals.length ; i++){
    
            Column col = table.getColumn(i); 
            int type = col.getType();
            //System.out.println(this.columnVals[i]); debugging

            if (this.columnVals[i]==null){
                this.offsets[i]= -1; 
                this.valueBuffer.writeShort(-1); 
                continue; 
            } else if (i == primary.getIndex()){
                this.offsets[i]=-2;
                this.valueBuffer.writeShort(-2); 
                if (type==0 || type==1){
                    this.keyBuffer.writeByte((int)this.columnVals[i]);
                    continue; 
                } else {
                    this.keyBuffer.writeBytes((String)this.columnVals[i]); 
                    continue; 
                }
            }

            this.offsets[i] = start;
            this.valueBuffer.writeShort(start);

            if (type==0 || type==1 || type==2){
                start+=col.getLength(); 
                continue; 
            } else {
                start +=((String)this.columnVals[i]).length(); 
                continue; 
            }
        }
        this.valueBuffer.writeShort(start);
        this.offsets[offsets.length-1]= start; 

        for(int i=0; i<this.columnVals.length ; i++){
            
            if(this.offsets[i]==-1 || this.offsets[i]==-2){
                continue; 
            }
            Column col = this.table.getColumn(i); 
            int type = col.getType();

            if (type==0){
                this.valueBuffer.writeInt((int)this.columnVals[i]);
                continue; 
            } else if (type==1){
                this.valueBuffer.writeDouble((int)this.columnVals[i]);
                continue;
            } else{
                this.valueBuffer.writeBytes((String)this.columnVals[i].toString()); 
                continue; 
            }
        }
    }
        
    /**
     * Returns the RowOutput used for the key portion of the marshalled row.
     *
     * @return  the key's RowOutput
     */
    public RowOutput getKeyBuffer() {
        return this.keyBuffer;
    }
    
    /**
     * Returns the RowOutput used for the value portion of the marshalled row.
     *
     * @return  the value's RowOutput
     */
    public RowOutput getValueBuffer() {
        return this.valueBuffer;
    }
    
    /**
     * Returns a String representation of this InsertRow object. 
     *
     * @return  a String for this InsertRow
     */
    public String toString() {
        return "offsets: " + Arrays.toString(this.offsets)
             + "\nkey buffer: " + this.keyBuffer
             + "\nvalue buffer: " + this.valueBuffer;
    }
}
