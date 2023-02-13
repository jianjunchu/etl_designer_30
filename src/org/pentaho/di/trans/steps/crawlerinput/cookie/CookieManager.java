package org.pentaho.di.trans.steps.crawler2020.cookie;
/*********************************************
    Copyright (c) 2001 by Daniel Matuschek
*********************************************/

import org.pentaho.di.trans.steps.crawler2020.cookie.Cookie;

import java.util.Vector;
import java.net.URL;
                                         
/**
 * This interface defines a container for storing cookies.
 * 
 * @author Daniel Matuschek 
 * @version $Id $
 */
public interface CookieManager
{

  /** 
   * Add this cookie. If there is already a cookie with the same name and
   * path it will be owerwritten by the new cookie.
   * @param cookie a Cookie that will be stored in this cookie manager
   */
  public void add(Cookie cookie);


  /** 
   * How many cookies are currently stored in this CookieManager ?
   * @return the number of stored Cookies
   */
  public int countCookies();


  /** 
   * Get the cookie values for the given URL.
   * @return a String containing a list of NAME=VALUE pairs (separated by
   * semicolon) containing all cookie values that are valid for the
   * given URL, <br/ >
   * null if no cookies can be found for this URL
   */
  public String cookiesForURL(URL u);


  /**
   * Remove all stored cookies
   */
  public void clear();
}
