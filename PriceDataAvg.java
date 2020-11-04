import java.io.Serializable;

public class PriceDataAvg implements Serializable{
	//"priceData":{"close":8063.07,"open":8052.58}
		
private static final long serialVersionUID = 1L;

	private Double close;
	private Double open;
	private int count;
	
	public Double getClose() {
		return close;
	}
	public void setClose(Double close) {
		this.close = close;
	}
	public Double getOpen() {
		return open;
	}
	public void setOpen(Double open) {
		this.open = open;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	public Double getOpenAvg() {
		 return this.open/this.count;
	}
	public Double getCloseAvg() {
		 return this.close/this.count;
	}
	public static PriceDataAvg sum(PriceDataAvg a, PriceDataAvg b) {
		int count = a.getCount()+b.getCount();
		double open = a.getOpen() + b.getOpen();
		double close = a.getClose() + b.getClose();
        return new PriceDataAvg(count,open,close);
    }
	public static PriceDataAvg diff(PriceDataAvg a, PriceDataAvg b) {
		if ((a.getCount()- b.getCount())==0) {
			double open = 0.0;
			double close = 0.0;
			int count = 0;
			return new PriceDataAvg(count,open,close);
		}
		else {
			int count = a.getCount() - b.getCount();
			double open = a.getOpen() -  b.getOpen();
			double close = a.getClose() - b.getClose();
	        return new PriceDataAvg(count,open,close);
		}
    }
    public  PriceDataAvg (int count, double open, double close) {
        this.count= count;
        this.open = open;
        this.close = close;
    }
    public PriceDataAvg ( double open, double close) {
        this.open = open;
        this.close = close;
        this.count = 1;
    }
	
	@Override
	public String toString() {
		return "OpeningPrice: " + this.getOpenAvg() + ", ClosingPrice: " + this.getCloseAvg() + ", Count: " + count;
	}

}
