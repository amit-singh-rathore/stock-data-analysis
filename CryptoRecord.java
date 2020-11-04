import java.io.Serializable;


public class CryptoRecord implements Serializable{
	private static final long serialVersionUID = 1L;
	private String symbol;
	private String timestamp;
	private PriceData priceData;
	
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public PriceData getPriceData() {
		return priceData;
	}
	public void setPriceData(PriceData priceData) {
		this.priceData = priceData;
	}
	
	@Override
	public String toString() {
		return "CryptoRecord [Symbol: " + symbol + ", Timestamp: " + timestamp + ", " +  priceData.toString() + " ]";
	}

}
