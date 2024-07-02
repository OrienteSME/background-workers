import datetime
import logging
import os
# import structlog
# import boto3
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    text,
)
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Base(DeclarativeBase):
    pass

class CoreOrder(Base):
    __tablename__ = "core_order"

    id = Column(Integer, primary_key=True, server_default=text("nextval('core_order_id_seq'::regclass)"))
    created_at = Column(DateTime(True), nullable=False)
    updated_at = Column(DateTime(True), nullable=False)
    order_id = Column(String(10), nullable=False, unique=True)
    channel_reference = Column(String(100))
    customer_first_name = Column(String(100), nullable=False)
    customer_last_name = Column(String(100), nullable=False)
    customer_email = Column(String(100), nullable=False)
    customer_contact_number = Column(String(100), nullable=False)
    payment_method = Column(String(32), nullable=False)
    pickup_date = Column(DateTime(True))
    delivery_date = Column(DateTime(True))
    pickup_address = Column(JSONB(astext_type=Text()))
    delivery_address = Column(JSONB(astext_type=Text()))
    total_price = Column(Numeric(19, 2), nullable=False)
    status = Column(String(50), nullable=False)
    raw_data = Column(JSONB(astext_type=Text()))
    user_id = Column(ForeignKey("core_user.id", deferrable=True, initially="DEFERRED"), nullable=False, index=True)
    delivered_at = Column(DateTime(True))
    picked_up_at = Column(DateTime(True))
    merchant_contact_number = Column(String(100), nullable=False)
    is_paid = Column(Boolean, nullable=False)
    tracking_number = Column(String(64))
    cancel_reason_code = Column(String(16))
    payment_channel = Column(String(64))
    payment_provider = Column(String(12))
    booked_at = Column(DateTime(True))
    discount = Column(Numeric(19, 2), nullable=False)
    other_fees = Column(Numeric(19, 2), nullable=False)
    remarks = Column(Text, nullable=False)
    shipping_method = Column(String(32))
    currency = Column(String(3), nullable=False)
    tracking_url = Column(String(255))
    transaction_fee = Column(Numeric(19, 2), nullable=False)
    _remittable_amount = Column(Numeric(19, 2))
    # voucher_id = Column(ForeignKey("vouchers_instance.id", deferrable=True, initially="DEFERRED"), index=True)
    voucher_code = Column(String(255))
    _voucher_discount = Column(Numeric(19, 2), nullable=False)
    shipping_subsidy = Column(Numeric(19, 2), nullable=False)
    # buyer_id = Column(ForeignKey("core_buyer.id", deferrable=True, initially="DEFERRED"), index=True)
    access_method = Column(String(100), nullable=False)
    shipping_fee = Column(Numeric(19, 2), nullable=False)
    expires_at = Column(DateTime(True))
    uid = Column(String(40))
    tender_amount = Column(Numeric(19, 2), nullable=False)
    actual_created_at = Column(DateTime(True))
    actual_updated_at = Column(DateTime(True))
    cashier_mode = Column(String(50), nullable=False)
    shipping_channel = Column(String(32))
    metadata_ = Column("metadata", JSONB(astext_type=Text()))
    soft_delete = Column(Boolean)
    _sub_total = Column(Numeric(19, 2))

    is_merchant_paid_transaction_fee = Column(Boolean)
    merchant_paid_transaction_fee = Column(Numeric(19, 2))
    notes = Column(String(64))
    sort_code = Column(String(20))
    sort_no = Column(String(10))

class PaymentsInvoice(Base):
    __tablename__ = "payments_invoice"

    id = Column(UUID, primary_key=True)
    external_id = Column(String(255))
    first_name = Column(String(255), nullable=False)
    last_name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    payment_method = Column(String(32), nullable=False)
    payment_provider = Column(String(32))
    payment_provider_url = Column(String(255))
    total_order_amount = Column(Numeric(19, 2), nullable=False)
    other_fees = Column(Numeric(19, 2), nullable=False)
    shipping_fee = Column(Numeric(19, 2), nullable=False)
    payment_fee = Column(Numeric(19, 2), nullable=False)
    payment_fee_vat = Column(Numeric(19, 2), nullable=False)
    payment_fee_total = Column(Numeric(19, 2), nullable=False)
    transaction_fee = Column(Numeric(19, 2), nullable=False)
    total_invoice_amount = Column(Numeric(19, 2), nullable=False)
    payment_created_at = Column(DateTime(True))
    payment_paid_at = Column(DateTime(True))
    created_at = Column(DateTime(True), nullable=False)
    updated_at = Column(DateTime(True), nullable=False)
    provider_response = Column(JSONB(astext_type=Text()))
    order_id = Column(ForeignKey("core_order.id", deferrable=True, initially="DEFERRED"), index=True)
    xenplatform_account_id = Column(
        ForeignKey("payments_xenplatformaccount.id", deferrable=True, initially="DEFERRED"), index=True
    )
    status = Column(String(16), nullable=False)
    currency = Column(String(3), nullable=False)
    discount = Column(Numeric(19, 2), nullable=False)
    voucher_code = Column(String(255))
    voucher_discount = Column(Numeric(19, 2), nullable=False)
    shipping_subsidy = Column(Numeric(19, 2), nullable=False)
    uid = Column(String(40))
    tender_amount = Column(Numeric(19, 2), nullable=False)
    actual_created_at = Column(DateTime(True))
    actual_updated_at = Column(DateTime(True))
    polling_status = Column(String(16), nullable=False)
    expired_at = Column(DateTime(True))

print("Connection string", os.environ.get("CONNECTION_STRING"))
engine = create_engine(os.environ.get("CONNECTION_STRING"), echo=True)

def run(event, context):
    session = Session(engine)
    try:
        current_time = datetime.datetime.now()
        query_limit = current_time - datetime.timedelta(days=7) #Check only expired from the past week
        
        # Retrieve invoices that have expired
        expired_invoices = session.query(PaymentsInvoice).filter(
            PaymentsInvoice.expired_at >= query_limit and
            PaymentsInvoice.expired_at <= current_time and 
            PaymentsInvoice.paid_at == None
        ).all()

        # Get order_ids from expired_invoices
        order_ids = [invoice.order_id for invoice in expired_invoices]
        
        # Retrieve orders corresponding to the expired invoices
        orders_with_expired_invoices = session.query(CoreOrder).filter(
            CoreOrder.order_id.in_(order_ids)
        ).all()

        for order in orders_with_expired_invoices:
            logger.info(f"Processing order ID: {order.order_id}")
            order.status = "Cancelled"
            session.add(order)
            logger.info(f"Order ID {order.order_id} status updated to Cancelled")
        
        session.commit()
        logger.info(f"Processed {len(orders_with_expired_invoices)} orders")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error processing orders: {e}")

