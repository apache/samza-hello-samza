package samza.examples.azure.data;

import java.io.Serializable;
import org.apache.avro.AvroRuntimeException;
import samza.examples.cookbook.data.PageView;

public class PageViewAvroRecord extends org.apache.avro.specific.SpecificRecordBase
    implements org.apache.avro.specific.SpecificRecord, Serializable {
  public final org.apache.avro.Schema SCHEMA = org.apache.avro.Schema.parse(
      "{\"type\":\"record\",\"name\":\"PageViewAvroRecord\",\"namespace\":\"org.apache.samza.examples.events\", \"fields\":[{\"name\": \"userId\", \"type\": \"string\"}, {\"name\": \"country\", \"type\": \"string\"}, {\"name\": \"pageId\", \"type\": \"string\"}]}");

  private String userId;
  private String country;
  private String pageId;

  public static PageViewAvroRecord buildPageViewRecord(PageView pageView) {
    PageViewAvroRecord record = new PageViewAvroRecord();
    record.userId = pageView.userId;
    record.country = pageView.country;
    record.pageId = pageView.pageId;
    return record;
  }
  public org.apache.avro.Schema getSchema() {
    return SCHEMA;
  }

  public java.lang.Object get(int field) {
    switch (field) {
      case 0: return userId;
      case 1: return country;
      case 2: return pageId;
      default: throw new AvroRuntimeException("bad index");
    }
  }

  public void put(int field, Object value) {
    switch (field) {
      case 0:
        userId = (String) value; break;
      case 1:
        country = (String) value; break;
      case 2:
        pageId = (String) value; break;
      default:
        throw new AvroRuntimeException("bad index");
    }
  }
}
