package ca.uwaterloo.cs.bigdata2016w.ColourfulBlank.assignment4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import tl.lin.data.array.ArrayListOfIntsWritable;
import java.util.Arrays;

/**
 * Representation of a graph node for PageRank.
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class PageRankNode implements Writable {
  public static enum Type {
    Complete((byte) 0),  // PageRank mass and adjacency list.
    Mass((byte) 1),      // PageRank mass only.
    Structure((byte) 2); // Adjacency list only.

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

  private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

  private Type type;
  private int nodeid;
  // private float pagerank;
  private float [] pageranks; //my change
  private ArrayListOfIntsWritable adjacency;

  private int dumbLengthTrancker;

  public PageRankNode() {}

  // public PageRankNode(int numOfSources) {
  //   pageranks = new float[numOfSources];
  //   dumbLengthTrancker = numOfSources;
  // }
  public void setPageRankLength(int l){
    dumbLengthTrancker = l;
    pageranks = new float[l];
  }
  public float getPageRank(int index) {
    // return pagerank;
    return pageranks[index];
  }

  public float [] getPageRankList(){//
    return pageranks;
  }

  public void setPageRank(float p, int index) {
    // this.pagerank = p;
    this.pageranks[index] = p;
  }
  public void setPageRankList(float [] pList){
    dumbLengthTrancker = pList.length;
    this.pageranks = pList;
  }
  public int getNodeId() {
    return nodeid;
    // return nodeids[index];
  }
  public void setNodeId(int n) {
    this.nodeid = n;
    // this.nodeids[index] = n;
  }

  public ArrayListOfIntsWritable getAdjacencyList() {//
    return adjacency;
  }

  public void setAdjacencyList(ArrayListOfIntsWritable list) {
    // this.adjacenyList = list;
    this.adjacency = list;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }
  public int getDumbLength(){
    dumbLengthTrancker = pageranks.length;
    return dumbLengthTrancker;
  }

  /**
   * Deserializes this object.
   *
   * @param in source for raw byte representation
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int b = in.readByte();
    type = mapping[b];
    dumbLengthTrancker = in.readInt();
    pageranks = new float [dumbLengthTrancker];
    nodeid = in.readInt();

    if (type.equals(Type.Mass)) {
      for (int i = 0; i < dumbLengthTrancker; i++){
        pageranks[i] = in.readFloat();  
      }
      return;
    }

    if (type.equals(Type.Complete)) {
      for (int i = 0; i < dumbLengthTrancker; i++){
        pageranks[i] = in.readFloat();  
      }
    }
    
    adjacency = new ArrayListOfIntsWritable();
    adjacency.readFields(in);
    
  }

  /**
   * Serializes this object.
   *
   * @param out where to write the raw byte representation
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(type.val);
    out.writeInt(dumbLengthTrancker);
    out.writeInt(nodeid);

    if (type.equals(Type.Mass)) {
      for (int i = 0; i < dumbLengthTrancker; i++){
          out.writeFloat(pageranks[i]);
      }
      // out.writeFloat(pagerank);
      return;
    }

    if (type.equals(Type.Complete)) {
      for (int i = 0; i < dumbLengthTrancker; i++){
          out.writeFloat(pageranks[i]);
      }
      // out.writeFloat(pagerank);
    }
    adjacency.write(out);
    
  }

  @Override
  public String toString() {
    // return String.format("{%d %.4f %s}", nodeid, pagerank, (adjacenyList == null ? "[]"
        // : adjacenyList.toString(10)));
    String retVal = String.format("{ID: %d ", nodeid);
    for (int i = 0; i < pageranks.length; i++){
      retVal = retVal + String.format(" PageRank"+i+": %.4f ", pageranks[i]);  
    }
    retVal = retVal + String.format("list: %s}", (adjacency == null ? "[]" : adjacency.toString(10)));
    return retVal;
  }

  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNode create(DataInput in) throws IOException {
    PageRankNode m = new PageRankNode();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNode create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
