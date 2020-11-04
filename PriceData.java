import java.io.Serializable;
import java.lang.Math; 

public class PriceData implements Serializable{
	//"priceData":{"close":8063.07,"high":8063.32,"low":8048.36,"open":8052.58,"volume":-983281.62}
		
private static final long serialVersionUID = 1L;
	
	private Double high;
	private Double low;
	private Double close;
	private Double open;
	private Double volume;
	
	public Double getHigh() {
		return high;
	}
	public void setHigh(Double high) {
		this.high = high;
	}
	public Double getClose() {
		return close;
	}
	public void setClose(Double close) {
		this.close = close;
	}
	public Double getLow() {
		return low;
	}
	public void setLow(Double low) {
		this.low = low;
	}
	public Double getOpen() {
		return open;
	}
	public void setOpen(Double open) {
		this.open = open;
	}
	public Double getVolume() {
		return Math.abs(volume);
	}
	public void setVolume(Double volume) {
		this.volume = Math.abs(volume);
	}
	
	@Override
	public String toString() {
		return "OpeningPrice: " + open + ", ClosingPrice: " + close + ", HighestPrice: " + high+", LowestPrice: " + low + ", Volume: " + volume;
	}

}
