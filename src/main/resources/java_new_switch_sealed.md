
Java 14

String type = switch (obj) {
    case null -> "Null value";
    case Integer i -> "Integer: " + i;
    case String s -> "String: " + s.toUpperCase();
    case Boolean b -> "Boolean: " + b;
    default -> "Unknown type";
};

String result = switch (payment) {
    case CardPayment c -> "Processing card: " + c.cardNumber() 
                          + " amount: " + c.amount();

    case UpiPayment u -> "Processing UPI: " + u.upiId() 
                         + " amount: " + u.amount();

    case WalletPayment w -> "Processing wallet: " + w.walletId() 
                            + " amount: " + w.amount();

};


Java 8
if (payment instanceof CardPayment) {
    CardPayment c = (CardPayment) payment;
    return "Processing card: " + c.getCardNumber();
} else if (payment instanceof UpiPayment) {
    UpiPayment u = (UpiPayment) payment;
    return "Processing UPI: " + u.getUpiId();
} else if (payment instanceof WalletPayment) {
    WalletPayment w = (WalletPayment) payment;
    return "Processing wallet: " + w.getWalletId();
}