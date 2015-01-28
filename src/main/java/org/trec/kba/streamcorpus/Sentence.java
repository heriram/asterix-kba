/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.trec.kba.streamcorpus;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sentence implements org.apache.thrift.TBase<Sentence, Sentence._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Sentence");

  private static final org.apache.thrift.protocol.TField TOKENS_FIELD_DESC = new org.apache.thrift.protocol.TField("tokens", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField LABELS_FIELD_DESC = new org.apache.thrift.protocol.TField("labels", org.apache.thrift.protocol.TType.MAP, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new SentenceStandardSchemeFactory());
    schemes.put(TupleScheme.class, new SentenceTupleSchemeFactory());
  }

  /**
   * tokens in this sentence
   */
  public List<Token> tokens; // required
  /**
   * array of instances of Label attached to this sentence, defaults to
   * an empty map
   */
  public Map<String,List<Label>> labels; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * tokens in this sentence
     */
    TOKENS((short)1, "tokens"),
    /**
     * array of instances of Label attached to this sentence, defaults to
     * an empty map
     */
    LABELS((short)2, "labels");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // TOKENS
          return TOKENS;
        case 2: // LABELS
          return LABELS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private _Fields optionals[] = {_Fields.LABELS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TOKENS, new org.apache.thrift.meta_data.FieldMetaData("tokens", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Token.class))));
    tmpMap.put(_Fields.LABELS, new org.apache.thrift.meta_data.FieldMetaData("labels", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING            , "AnnotatorID"), 
            new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
                new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Label.class)))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Sentence.class, metaDataMap);
  }

  public Sentence() {
    this.tokens = new ArrayList<Token>();

    this.labels = new HashMap<String,List<Label>>();

  }

  public Sentence(
    List<Token> tokens)
  {
    this();
    this.tokens = tokens;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Sentence(Sentence other) {
    if (other.isSetTokens()) {
      List<Token> __this__tokens = new ArrayList<Token>();
      for (Token other_element : other.tokens) {
        __this__tokens.add(new Token(other_element));
      }
      this.tokens = __this__tokens;
    }
    if (other.isSetLabels()) {
      Map<String,List<Label>> __this__labels = new HashMap<String,List<Label>>();
      for (Map.Entry<String, List<Label>> other_element : other.labels.entrySet()) {

        String other_element_key = other_element.getKey();
        List<Label> other_element_value = other_element.getValue();

        String __this__labels_copy_key = other_element_key;

        List<Label> __this__labels_copy_value = new ArrayList<Label>();
        for (Label other_element_value_element : other_element_value) {
          __this__labels_copy_value.add(new Label(other_element_value_element));
        }

        __this__labels.put(__this__labels_copy_key, __this__labels_copy_value);
      }
      this.labels = __this__labels;
    }
  }

  public Sentence deepCopy() {
    return new Sentence(this);
  }

  @Override
  public void clear() {
    this.tokens = new ArrayList<Token>();

    this.labels = new HashMap<String,List<Label>>();

  }

  public int getTokensSize() {
    return (this.tokens == null) ? 0 : this.tokens.size();
  }

  public java.util.Iterator<Token> getTokensIterator() {
    return (this.tokens == null) ? null : this.tokens.iterator();
  }

  public void addToTokens(Token elem) {
    if (this.tokens == null) {
      this.tokens = new ArrayList<Token>();
    }
    this.tokens.add(elem);
  }

  /**
   * tokens in this sentence
   */
  public List<Token> getTokens() {
    return this.tokens;
  }

  /**
   * tokens in this sentence
   */
  public Sentence setTokens(List<Token> tokens) {
    this.tokens = tokens;
    return this;
  }

  public void unsetTokens() {
    this.tokens = null;
  }

  /** Returns true if field tokens is set (has been assigned a value) and false otherwise */
  public boolean isSetTokens() {
    return this.tokens != null;
  }

  public void setTokensIsSet(boolean value) {
    if (!value) {
      this.tokens = null;
    }
  }

  public int getLabelsSize() {
    return (this.labels == null) ? 0 : this.labels.size();
  }

  public void putToLabels(String key, List<Label> val) {
    if (this.labels == null) {
      this.labels = new HashMap<String,List<Label>>();
    }
    this.labels.put(key, val);
  }

  /**
   * array of instances of Label attached to this sentence, defaults to
   * an empty map
   */
  public Map<String,List<Label>> getLabels() {
    return this.labels;
  }

  /**
   * array of instances of Label attached to this sentence, defaults to
   * an empty map
   */
  public Sentence setLabels(Map<String,List<Label>> labels) {
    this.labels = labels;
    return this;
  }

  public void unsetLabels() {
    this.labels = null;
  }

  /** Returns true if field labels is set (has been assigned a value) and false otherwise */
  public boolean isSetLabels() {
    return this.labels != null;
  }

  public void setLabelsIsSet(boolean value) {
    if (!value) {
      this.labels = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TOKENS:
      if (value == null) {
        unsetTokens();
      } else {
        setTokens((List<Token>)value);
      }
      break;

    case LABELS:
      if (value == null) {
        unsetLabels();
      } else {
        setLabels((Map<String,List<Label>>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TOKENS:
      return getTokens();

    case LABELS:
      return getLabels();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TOKENS:
      return isSetTokens();
    case LABELS:
      return isSetLabels();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Sentence)
      return this.equals((Sentence)that);
    return false;
  }

  public boolean equals(Sentence that) {
    if (that == null)
      return false;

    boolean this_present_tokens = true && this.isSetTokens();
    boolean that_present_tokens = true && that.isSetTokens();
    if (this_present_tokens || that_present_tokens) {
      if (!(this_present_tokens && that_present_tokens))
        return false;
      if (!this.tokens.equals(that.tokens))
        return false;
    }

    boolean this_present_labels = true && this.isSetLabels();
    boolean that_present_labels = true && that.isSetLabels();
    if (this_present_labels || that_present_labels) {
      if (!(this_present_labels && that_present_labels))
        return false;
      if (!this.labels.equals(that.labels))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(Sentence other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    Sentence typedOther = (Sentence)other;

    lastComparison = Boolean.valueOf(isSetTokens()).compareTo(typedOther.isSetTokens());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTokens()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tokens, typedOther.tokens);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLabels()).compareTo(typedOther.isSetLabels());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLabels()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.labels, typedOther.labels);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Sentence(");
    boolean first = true;

    sb.append("tokens:");
    if (this.tokens == null) {
      sb.append("null");
    } else {
      sb.append(this.tokens);
    }
    first = false;
    if (isSetLabels()) {
      if (!first) sb.append(", ");
      sb.append("labels:");
      if (this.labels == null) {
        sb.append("null");
      } else {
        sb.append(this.labels);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class SentenceStandardSchemeFactory implements SchemeFactory {
    public SentenceStandardScheme getScheme() {
      return new SentenceStandardScheme();
    }
  }

  private static class SentenceStandardScheme extends StandardScheme<Sentence> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Sentence struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TOKENS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list38 = iprot.readListBegin();
                struct.tokens = new ArrayList<Token>(_list38.size);
                for (int _i39 = 0; _i39 < _list38.size; ++_i39)
                {
                  Token _elem40; // required
                  _elem40 = new Token();
                  _elem40.read(iprot);
                  struct.tokens.add(_elem40);
                }
                iprot.readListEnd();
              }
              struct.setTokensIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // LABELS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map41 = iprot.readMapBegin();
                struct.labels = new HashMap<String,List<Label>>(2*_map41.size);
                for (int _i42 = 0; _i42 < _map41.size; ++_i42)
                {
                  String _key43; // required
                  List<Label> _val44; // required
                  _key43 = iprot.readString();
                  {
                    org.apache.thrift.protocol.TList _list45 = iprot.readListBegin();
                    _val44 = new ArrayList<Label>(_list45.size);
                    for (int _i46 = 0; _i46 < _list45.size; ++_i46)
                    {
                      Label _elem47; // required
                      _elem47 = new Label();
                      _elem47.read(iprot);
                      _val44.add(_elem47);
                    }
                    iprot.readListEnd();
                  }
                  struct.labels.put(_key43, _val44);
                }
                iprot.readMapEnd();
              }
              struct.setLabelsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, Sentence struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.tokens != null) {
        oprot.writeFieldBegin(TOKENS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.tokens.size()));
          for (Token _iter48 : struct.tokens)
          {
            _iter48.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.labels != null) {
        if (struct.isSetLabels()) {
          oprot.writeFieldBegin(LABELS_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.LIST, struct.labels.size()));
            for (Map.Entry<String, List<Label>> _iter49 : struct.labels.entrySet())
            {
              oprot.writeString(_iter49.getKey());
              {
                oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, _iter49.getValue().size()));
                for (Label _iter50 : _iter49.getValue())
                {
                  _iter50.write(oprot);
                }
                oprot.writeListEnd();
              }
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SentenceTupleSchemeFactory implements SchemeFactory {
    public SentenceTupleScheme getScheme() {
      return new SentenceTupleScheme();
    }
  }

  private static class SentenceTupleScheme extends TupleScheme<Sentence> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Sentence struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetTokens()) {
        optionals.set(0);
      }
      if (struct.isSetLabels()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetTokens()) {
        {
          oprot.writeI32(struct.tokens.size());
          for (Token _iter51 : struct.tokens)
          {
            _iter51.write(oprot);
          }
        }
      }
      if (struct.isSetLabels()) {
        {
          oprot.writeI32(struct.labels.size());
          for (Map.Entry<String, List<Label>> _iter52 : struct.labels.entrySet())
          {
            oprot.writeString(_iter52.getKey());
            {
              oprot.writeI32(_iter52.getValue().size());
              for (Label _iter53 : _iter52.getValue())
              {
                _iter53.write(oprot);
              }
            }
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Sentence struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list54 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.tokens = new ArrayList<Token>(_list54.size);
          for (int _i55 = 0; _i55 < _list54.size; ++_i55)
          {
            Token _elem56; // required
            _elem56 = new Token();
            _elem56.read(iprot);
            struct.tokens.add(_elem56);
          }
        }
        struct.setTokensIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TMap _map57 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.LIST, iprot.readI32());
          struct.labels = new HashMap<String,List<Label>>(2*_map57.size);
          for (int _i58 = 0; _i58 < _map57.size; ++_i58)
          {
            String _key59; // required
            List<Label> _val60; // required
            _key59 = iprot.readString();
            {
              org.apache.thrift.protocol.TList _list61 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
              _val60 = new ArrayList<Label>(_list61.size);
              for (int _i62 = 0; _i62 < _list61.size; ++_i62)
              {
                Label _elem63; // required
                _elem63 = new Label();
                _elem63.read(iprot);
                _val60.add(_elem63);
              }
            }
            struct.labels.put(_key59, _val60);
          }
        }
        struct.setLabelsIsSet(true);
      }
    }
  }

}

