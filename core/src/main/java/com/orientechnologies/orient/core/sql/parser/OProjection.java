/* Generated By:JJTree: Do not edit this line. OProjection.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import java.util.List;

public class OProjection extends SimpleNode {

  List<OProjectionItem> items;

  public OProjection(int id) {
    super(id);
  }

  public OProjection(OrientSql p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  public List<OProjectionItem> getItems() {
    return items;
  }

  public void setItems(List<OProjectionItem> items) {
    this.items = items;
  }
}
/* JavaCC - OriginalChecksum=3a650307b53bae626dc063c4b35e62c3 (do not edit this line) */
