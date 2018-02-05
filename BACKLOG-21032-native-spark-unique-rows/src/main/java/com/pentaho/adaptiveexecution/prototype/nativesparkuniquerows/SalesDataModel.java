package com.pentaho.adaptiveexecution.prototype.nativesparkuniquerows;

import java.io.Serializable;

public class SalesDataModel implements Serializable {
  private String orderNumber;
  private int quantityOrdered;
  private double priceEach;
  private int orderLineNumber;
  private double sale;
  private String status;
  private int qtr_id;
  private String city;
  private String state;
  private String postalCode;
  private String country;

  public SalesDataModel() {
    super();
  }

  @Override public String toString() {
    return "SalesDataModel{" +
      "orderNumber='" + orderNumber + '\'' +
      ", quantityOrdered=" + quantityOrdered +
      ", priceEach=" + priceEach +
      ", orderLineNumber=" + orderLineNumber +
      ", sale=" + sale +
      ", status='" + status + '\'' +
      ", qtr_id=" + qtr_id +
      ", city='" + city + '\'' +
      ", state='" + state + '\'' +
      ", postalCode='" + postalCode + '\'' +
      ", country='" + country + '\'' +
      '}';
  }

  public SalesDataModel( String orderNumber, String quantityOrdered, String priceEach, String orderLineNumber, String sale,
                         String status, String qtr_id, String city, String state, String postalCode,
                         String country ) {

    this.orderNumber = orderNumber;
    this.quantityOrdered = Integer.valueOf( quantityOrdered );
    this.priceEach = Double.valueOf( priceEach );
    this.orderLineNumber = Integer.valueOf( orderLineNumber );
    this.sale = Double.valueOf( sale );
    this.status = status;
    this.qtr_id = Integer.valueOf( qtr_id );
    this.city = city;
    this.state = state;
    this.postalCode = postalCode;
    this.country = country;
  }

  public String getOrderNumber() {
    return orderNumber;
  }

  public void setOrderNumber( String orderNumber ) {
    this.orderNumber = orderNumber;
  }

  public int getQuantityOrdered() {
    return quantityOrdered;
  }

  public void setQuantityOrdered( int quantityOrdered ) {
    this.quantityOrdered = quantityOrdered;
  }

  public double getPriceEach() {
    return priceEach;
  }

  public void setPriceEach( double priceEach ) {
    this.priceEach = priceEach;
  }

  public int getOrderLineNumber() {
    return orderLineNumber;
  }

  public void setOrderLineNumber( int orderLineNumber ) {
    this.orderLineNumber = orderLineNumber;
  }

  public double getSale() {
    return sale;
  }

  public void setSale( double sale ) {
    this.sale = sale;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus( String status ) {
    this.status = status;
  }

  public int getQtr_id() {
    return qtr_id;
  }

  public void setQtr_id( int qtr_id ) {
    this.qtr_id = qtr_id;
  }

  public String getCity() {
    return city;
  }

  public void setCity( String city ) {
    this.city = city;
  }

  public String getState() {
    return state;
  }

  public void setState( String state ) {
    this.state = state;
  }

  public String getPostalCode() {
    return postalCode;
  }

  public void setPostalCode( String postalCode ) {
    this.postalCode = postalCode;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry( String country ) {
    this.country = country;
  }
}
