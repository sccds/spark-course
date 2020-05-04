/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.twq.spark.session;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class TrackerLog extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"TrackerLog\",\"namespace\":\"com.twq.spark.session\",\"fields\":[{\"name\":\"log_type\",\"type\":\"string\"},{\"name\":\"log_server_time\",\"type\":\"string\"},{\"name\":\"cookie\",\"type\":\"string\"},{\"name\":\"ip\",\"type\":\"string\"},{\"name\":\"url\",\"type\":\"string\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence log_type;
  @Deprecated public java.lang.CharSequence log_server_time;
  @Deprecated public java.lang.CharSequence cookie;
  @Deprecated public java.lang.CharSequence ip;
  @Deprecated public java.lang.CharSequence url;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public TrackerLog() {}

  /**
   * All-args constructor.
   */
  public TrackerLog(java.lang.CharSequence log_type, java.lang.CharSequence log_server_time, java.lang.CharSequence cookie, java.lang.CharSequence ip, java.lang.CharSequence url) {
    this.log_type = log_type;
    this.log_server_time = log_server_time;
    this.cookie = cookie;
    this.ip = ip;
    this.url = url;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return log_type;
    case 1: return log_server_time;
    case 2: return cookie;
    case 3: return ip;
    case 4: return url;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: log_type = (java.lang.CharSequence)value$; break;
    case 1: log_server_time = (java.lang.CharSequence)value$; break;
    case 2: cookie = (java.lang.CharSequence)value$; break;
    case 3: ip = (java.lang.CharSequence)value$; break;
    case 4: url = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'log_type' field.
   */
  public java.lang.CharSequence getLogType() {
    return log_type;
  }

  /**
   * Sets the value of the 'log_type' field.
   * @param value the value to set.
   */
  public void setLogType(java.lang.CharSequence value) {
    this.log_type = value;
  }

  /**
   * Gets the value of the 'log_server_time' field.
   */
  public java.lang.CharSequence getLogServerTime() {
    return log_server_time;
  }

  /**
   * Sets the value of the 'log_server_time' field.
   * @param value the value to set.
   */
  public void setLogServerTime(java.lang.CharSequence value) {
    this.log_server_time = value;
  }

  /**
   * Gets the value of the 'cookie' field.
   */
  public java.lang.CharSequence getCookie() {
    return cookie;
  }

  /**
   * Sets the value of the 'cookie' field.
   * @param value the value to set.
   */
  public void setCookie(java.lang.CharSequence value) {
    this.cookie = value;
  }

  /**
   * Gets the value of the 'ip' field.
   */
  public java.lang.CharSequence getIp() {
    return ip;
  }

  /**
   * Sets the value of the 'ip' field.
   * @param value the value to set.
   */
  public void setIp(java.lang.CharSequence value) {
    this.ip = value;
  }

  /**
   * Gets the value of the 'url' field.
   */
  public java.lang.CharSequence getUrl() {
    return url;
  }

  /**
   * Sets the value of the 'url' field.
   * @param value the value to set.
   */
  public void setUrl(java.lang.CharSequence value) {
    this.url = value;
  }

  /** Creates a new TrackerLog RecordBuilder */
  public static com.twq.spark.session.TrackerLog.Builder newBuilder() {
    return new com.twq.spark.session.TrackerLog.Builder();
  }
  
  /** Creates a new TrackerLog RecordBuilder by copying an existing Builder */
  public static com.twq.spark.session.TrackerLog.Builder newBuilder(com.twq.spark.session.TrackerLog.Builder other) {
    return new com.twq.spark.session.TrackerLog.Builder(other);
  }
  
  /** Creates a new TrackerLog RecordBuilder by copying an existing TrackerLog instance */
  public static com.twq.spark.session.TrackerLog.Builder newBuilder(com.twq.spark.session.TrackerLog other) {
    return new com.twq.spark.session.TrackerLog.Builder(other);
  }
  
  /**
   * RecordBuilder for TrackerLog instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<TrackerLog>
    implements org.apache.avro.data.RecordBuilder<TrackerLog> {

    private java.lang.CharSequence log_type;
    private java.lang.CharSequence log_server_time;
    private java.lang.CharSequence cookie;
    private java.lang.CharSequence ip;
    private java.lang.CharSequence url;

    /** Creates a new Builder */
    private Builder() {
      super(com.twq.spark.session.TrackerLog.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.twq.spark.session.TrackerLog.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.log_type)) {
        this.log_type = data().deepCopy(fields()[0].schema(), other.log_type);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.log_server_time)) {
        this.log_server_time = data().deepCopy(fields()[1].schema(), other.log_server_time);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.cookie)) {
        this.cookie = data().deepCopy(fields()[2].schema(), other.cookie);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.ip)) {
        this.ip = data().deepCopy(fields()[3].schema(), other.ip);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.url)) {
        this.url = data().deepCopy(fields()[4].schema(), other.url);
        fieldSetFlags()[4] = true;
      }
    }
    
    /** Creates a Builder by copying an existing TrackerLog instance */
    private Builder(com.twq.spark.session.TrackerLog other) {
            super(com.twq.spark.session.TrackerLog.SCHEMA$);
      if (isValidValue(fields()[0], other.log_type)) {
        this.log_type = data().deepCopy(fields()[0].schema(), other.log_type);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.log_server_time)) {
        this.log_server_time = data().deepCopy(fields()[1].schema(), other.log_server_time);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.cookie)) {
        this.cookie = data().deepCopy(fields()[2].schema(), other.cookie);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.ip)) {
        this.ip = data().deepCopy(fields()[3].schema(), other.ip);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.url)) {
        this.url = data().deepCopy(fields()[4].schema(), other.url);
        fieldSetFlags()[4] = true;
      }
    }

    /** Gets the value of the 'log_type' field */
    public java.lang.CharSequence getLogType() {
      return log_type;
    }
    
    /** Sets the value of the 'log_type' field */
    public com.twq.spark.session.TrackerLog.Builder setLogType(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.log_type = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'log_type' field has been set */
    public boolean hasLogType() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'log_type' field */
    public com.twq.spark.session.TrackerLog.Builder clearLogType() {
      log_type = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'log_server_time' field */
    public java.lang.CharSequence getLogServerTime() {
      return log_server_time;
    }
    
    /** Sets the value of the 'log_server_time' field */
    public com.twq.spark.session.TrackerLog.Builder setLogServerTime(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.log_server_time = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'log_server_time' field has been set */
    public boolean hasLogServerTime() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'log_server_time' field */
    public com.twq.spark.session.TrackerLog.Builder clearLogServerTime() {
      log_server_time = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'cookie' field */
    public java.lang.CharSequence getCookie() {
      return cookie;
    }
    
    /** Sets the value of the 'cookie' field */
    public com.twq.spark.session.TrackerLog.Builder setCookie(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.cookie = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'cookie' field has been set */
    public boolean hasCookie() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'cookie' field */
    public com.twq.spark.session.TrackerLog.Builder clearCookie() {
      cookie = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'ip' field */
    public java.lang.CharSequence getIp() {
      return ip;
    }
    
    /** Sets the value of the 'ip' field */
    public com.twq.spark.session.TrackerLog.Builder setIp(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.ip = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'ip' field has been set */
    public boolean hasIp() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'ip' field */
    public com.twq.spark.session.TrackerLog.Builder clearIp() {
      ip = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /** Gets the value of the 'url' field */
    public java.lang.CharSequence getUrl() {
      return url;
    }
    
    /** Sets the value of the 'url' field */
    public com.twq.spark.session.TrackerLog.Builder setUrl(java.lang.CharSequence value) {
      validate(fields()[4], value);
      this.url = value;
      fieldSetFlags()[4] = true;
      return this; 
    }
    
    /** Checks whether the 'url' field has been set */
    public boolean hasUrl() {
      return fieldSetFlags()[4];
    }
    
    /** Clears the value of the 'url' field */
    public com.twq.spark.session.TrackerLog.Builder clearUrl() {
      url = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    public TrackerLog build() {
      try {
        TrackerLog record = new TrackerLog();
        record.log_type = fieldSetFlags()[0] ? this.log_type : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.log_server_time = fieldSetFlags()[1] ? this.log_server_time : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.cookie = fieldSetFlags()[2] ? this.cookie : (java.lang.CharSequence) defaultValue(fields()[2]);
        record.ip = fieldSetFlags()[3] ? this.ip : (java.lang.CharSequence) defaultValue(fields()[3]);
        record.url = fieldSetFlags()[4] ? this.url : (java.lang.CharSequence) defaultValue(fields()[4]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}