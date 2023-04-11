from flask import Flask, request, g
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask import jsonify
import json
import eth_account
import algosdk
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import load_only
from datetime import datetime
import sys

from models import Base, Order, Log

engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

app = Flask(__name__)


@app.before_request
def create_session():
    g.session = scoped_session(DBSession)


@app.teardown_appcontext
def shutdown_session(response_or_exc):
    sys.stdout.flush()
    g.session.commit()
    g.session.remove()


""" Suggested helper methods """


def check_sig(payload, sig):

    platform = payload['platform']
    pk = payload['sender_pk']

    verification_result = False

    if platform == 'Ethereum':
        eth_encoded_msg = eth_account.messages.encode_defunct(text=payload)
        if eth_account.Account.recover_message(eth_encoded_msg, signature=sig) == pk:
            verification_result = True

    elif platform == 'Algorand':
        if algosdk.util.verify_bytes(payload.encode('utf-8'), sig, pk):
            verification_result = True

    return verification_result


def fill_order(order, txes=[]):
    buy_currency = order['buy_currency']
    sell_currency = order['sell_currency']
    buy_amount = order['buy_amount']
    sell_amount = order['sell_amount']
    sender_pk = order['sender_pk']
    receiver_pk = order['receiver_pk']
    implied_exchange_rate = buy_amount / sell_amount
    new_order_sell_rate = sell_amount / buy_amount

    order_obj = Order(sender_pk=order['sender_pk'], receiver_pk=order['receiver_pk'],
                      buy_currency=order['buy_currency'], sell_currency=order['sell_currency'],
                      buy_amount=order['buy_amount'], sell_amount=order['sell_amount'])
    g.session.add(order_obj)
    g.session.commit()

    new_order_ID = order_obj.id

    query = g.session.query(Order).filter(
        Order.filled == None, Order.buy_currency == sell_currency, Order.sell_currency == buy_currency
    )
    result = g.session.execute(query)
    amount = 0
    id_order_matched = 0

    for order in result.scalars().all():
        existing_order_exchange_rate = order.sell_amount / order.buy_amount
        if existing_order_exchange_rate >= implied_exchange_rate:
            if order.sell_amount > amount:
                amount = sell_amount
                id_order_matched = order.id
    #pass

    if id_order_matched != 0:
        existing_order_sell_amount = 0
        existing_order_buy_amount = 0
        existing_order_sell_rate = 0
        existing_order_sender_pk = None
        existing_order_receiver_pk = None

        now = datetime.now()

        query1 = g.session.query(Order).filter(Order.id == id_order_matched)
        result1 = g.session.execute(query1)
        for order in result1.scalars().all():
            order.filled = now
            order.counterparty_id = new_order_ID
            existing_order_sell_amount = order.sell_amount
            existing_order_buy_amount = order.buy_amount
            existing_order_sender_pk = order.sender_pk
            existing_order_receiver_pk = order.receiver_pk
            existing_order_sell_rate = existing_order_sell_amount / existing_order_buy_amount
            g.session.commit()

        query2 = g.session.query(Order).filter(Order.id == new_order_ID)
        result2 = g.session.execute(query2)
        for order in result2.scalars().all():
            order.filled = now
            order.counterparty_id = id_order_matched
            g.session.commit()

        child_order_obj = None

        if existing_order_sell_amount < buy_amount:
            final_sell_amount = existing_order_sell_amount
            final_buy_amount = existing_order_buy_amount

            buy_amount = buy_amount - final_buy_amount
            sell_amount = buy_amount * new_order_sell_rate
            creator_id = new_order_ID

            child_order = {}
            child_order['sender_pk'] = sender_pk
            child_order['receiver_pk'] = receiver_pk
            child_order['buy_currency'] = buy_currency
            child_order['sell_currency'] = sell_currency
            child_order['buy_amount'] = buy_amount
            child_order['sell_amount'] = sell_amount
            child_order['creator_id'] = creator_id

            child_order_obj = Order(sender_pk=child_order['sender_pk'],
                                    receiver_pk=child_order['receiver_pk'],
                                    buy_currency=child_order['buy_currency'],
                                    sell_currency=child_order['sell_currency'],
                                    buy_amount=child_order['buy_amount'],
                                    sell_amount=child_order['sell_amount'],
                                    creator_id=child_order['creator_id'])
            g.session.add(child_order_obj)
            g.session.commit()


        elif existing_order_sell_amount > buy_amount:
            final_sell_amount = sell_amount
            final_buy_amount = buy_amount

            buy_currency_original = buy_currency
            sender_pk = existing_order_sender_pk
            receiver_pk = existing_order_receiver_pk
            buy_currency = sell_currency
            sell_currency = buy_currency_original
            buy_amount = existing_order_buy_amount - final_sell_amount
            sell_amount = buy_amount * existing_order_sell_rate
            creator_id = id_order_matched

            child_order = {}
            child_order['sender_pk'] = sender_pk
            child_order['receiver_pk'] = receiver_pk
            child_order['buy_currency'] = buy_currency
            child_order['sell_currency'] = sell_currency
            child_order['buy_amount'] = buy_amount
            child_order['sell_amount'] = sell_amount
            child_order['creator_id'] = creator_id

            child_order_obj = Order(sender_pk=child_order['sender_pk'],
                                    receiver_pk=child_order['receiver_pk'],
                                    buy_currency=child_order['buy_currency'],
                                    sell_currency=child_order['sell_currency'],
                                    buy_amount=child_order['buy_amount'],
                                    sell_amount=child_order['sell_amount'],
                                    creator_id=child_order['creator_id'])

            g.session.add(child_order_obj)
            g.session.commit()

        txes.append(order)
        txes.append(child_order)

def log_message(d):
    # Takes input dictionary d and writes it to the Log table
    # Hint: use json.dumps or str() to get it in a nice string form
    log_obj = Log(message=Log['message'])
    g.session.add(log_obj)
    g.session.commit()


""" End of helper methods """


@app.route('/trade', methods=['POST'])
def trade():
    print("In trade endpoint")
    if request.method == "POST":
        content = request.get_json(silent=True)
        print(f"content = {json.dumps(content)}")
        columns = ["sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount", "sell_amount", "platform"]
        fields = ["sig", "payload"]

        for field in fields:
            if not field in content.keys():
                print(f"{field} not received by Trade")
                print(json.dumps(content))
                log_message(content)
                return jsonify(False)

        for column in columns:
            if not column in content['payload'].keys():
                print(f"{column} not received by Trade")
                print(json.dumps(content))
                log_message(content)
                return jsonify(False)

        # Your code here
        # Note that you can access the database session using g.session

        # TODO: Check the signature
        result = False
        json_string = json.dumps(content)
        contentPyth = json.loads(json_string)

        signature = contentPyth['sig']
        payload = json.dumps(contentPyth['payload'])

        verification_result=check_sig(payload, signature)

        # TODO: Add the order to the database
        sender_pk = contentPyth['payload']['sender_pk']
        receiver_pk = contentPyth['payload']['receiver_pk']
        buy_currency = contentPyth['payload']['buy_currency']
        sell_currency = contentPyth['payload']['sell_currency']
        buy_amount = contentPyth['payload']['buy_amount']
        sell_amount = contentPyth['payload']['sell_amount']

        if verification_result == True:
            order = {}
            order['sender_pk'] = sender_pk
            order['receiver_pk'] = receiver_pk
            order['buy_currency'] = buy_currency
            order['sell_currency'] = sell_currency
            order['buy_amount'] = buy_amount
            order['sell_amount'] = sell_amount

        # TODO: Fill the order
            txes = []
            fill_order(order, txes)
            result = True
        else:
            log_message(payload)
        # TODO: Be sure to return jsonify(True) or jsonify(False) depending on if the method was successful
        return (jsonify(result))

@app.route('/order_book')
def order_book():
    # Your code here
    # Note that you can access the database session using g.session
    keyList = ['sender_pk', 'receiver_pk', 'buy_currency', 'sell_currency', 'buy_amount', 'sell_amount', 'signature']
    query = g.session.query(Order)
    query_result = g.session.execute(query)
    initial_result = []
    for order in query_result.scalars().all():
        order_dict = dict.fromkeys(keyList)
        order_dict['sender_pk'] = order.sender_pk
        order_dict['receiver_pk'] = order.receiver_pk
        order_dict['buy_currency'] = order.buy_currency
        order_dict['sell_currency'] = order.sell_currency
        order_dict['buy_amount'] = order.buy_amount
        order_dict['sell_amount'] = order.sell_amount
        order_dict['signature'] = order.signature
        initial_result.append(order_dict)

    # Note that you can access the database session using g.session
    keyList2 = ['data']
    result = dict.fromkeys(keyList2)
    result['data'] = initial_result
    return jsonify(result)

if __name__ == '__main__':
    app.run(port='5002')
