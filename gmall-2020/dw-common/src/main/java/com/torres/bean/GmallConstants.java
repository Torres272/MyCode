package com.torres.bean;

public class GmallConstants {
    //启动日志主题
    public static final String KAFKA_TOPIC_STARTUP="GMALL_STARTUP";

    //事件日志主题
    public static final String KAFKA_TOPIC_EVENT="GMALL_EVENT";

    //订单数据主题
    public static final String GMALL_ORDER_INFO_TOPIC = "gmall_order_info";

    //定义订单详情主题
    public static final String GMALL_ORDER_DETAIL_TOPIC = "gmall_order_detail";

    //定义用户主题
    public static final String GMALL_USER_INFO_TOPIC = "gmall_user_info";

    //定义预警日志的ES index
    public static final String GMALL_ALERT_INFO_INDEX = "gmall_coupon_alert";

    //定义订单销售详情ES Index
    public static final String GMALL_SALE_DETAIL_INDEX = "gmall190826_sale_detail";

}
