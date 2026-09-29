use rate_limited_order_worker::{apply_order_job, OrderJob, OrderStage};

#[test]
fn issuing_a_receipt_schedules_the_customer_update() {
    let update = apply_order_job(OrderJob {
        order_id: "order-1042".into(),
        stage: OrderStage::ReceiptIssued,
        customer_email: "buyer@example.com".into(),
    });

    assert_eq!(update.order_id, "order-1042");
    assert_eq!(update.completed, OrderStage::ReceiptIssued);
    assert_eq!(update.next, Some(OrderStage::CustomerUpdated));
    assert_eq!(update.detail, "receipt issued for buyer@example.com");
}
