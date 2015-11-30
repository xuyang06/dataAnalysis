package myudfs;

import java.io.IOException;
import java.util.HashMap;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.WrappedIOException;


public class Parser extends EvalFunc<DataBag>{
	TupleFactory tupleFactory = TupleFactory.getInstance();
    BagFactory bagFactory = BagFactory.getInstance();

	
    protected void addTuple(DataBag outputBag, String itemName, int number) throws ExecException {
        Tuple outputTuple = tupleFactory.newTuple();
        outputTuple.append(itemName);
        outputTuple.append(number);
        outputBag.add(outputTuple);
    }
    
	public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
        	DataBag output = bagFactory.newDefaultBag();
            String line = (String)input.get(0);
            HashMap<String, Integer> tokenNumberMap = new HashMap<String, Integer>();
    		tokenNumberMap.put("hackathon", 0);
    		tokenNumberMap.put("dec", 0);
    		tokenNumberMap.put("chicago", 0);
    		tokenNumberMap.put("java", 0);
    		String[] tweetArray = line.split(",",4);
    		String[] tweetArrayItem0Array = tweetArray[0].split("-");
    		formTokenNumberMap(tokenNumberMap, tweetArrayItem0Array);
    		String[] tweetArrayItem1Array = {tweetArray[1]};
    		formTokenNumberMap(tokenNumberMap, tweetArrayItem1Array);
    		String[] tweetArrayItem2Array = {tweetArray[2]};
    		formTokenNumberMap(tokenNumberMap, tweetArrayItem2Array);
    		String[] tweetArrayItem3Array = tweetArray[3].split("-|/|:| ");
    		formTokenNumberMap(tokenNumberMap, tweetArrayItem3Array);
    		for(String tweetKey: tokenNumberMap.keySet()){
    			int tweetNumber = tokenNumberMap.get(tweetKey);
    			/*if (tweetNumber > 0){
    				for(int i = 0; i < tweetNumber; i ++){
    					this.addTuple(output, tweetKey);
    				}
    			}*/
    			this.addTuple(output, tweetKey, tweetNumber);
    		}
    		
            return output;
        }catch(Exception e){
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
    }
	
	public void formTokenNumberMap(HashMap<String, Integer> tokenNumberMap, String[] strItemArray){
		for(int i = 0; i < strItemArray.length; i ++){
			String strItem = strItemArray[i].trim();
			if (strItem.endsWith(",")){
				strItem = strItem.substring(0, strItem.length()-1);
			}
			strItem = strItem.toLowerCase();
			if (tokenNumberMap.containsKey(strItem)){
				tokenNumberMap.put(strItem, 1);
			}
		}
	}
	
	
	@Override
    public Schema outputSchema(Schema input) {
        try {
            if (input.size() != 1) {
                throw new RuntimeException("Expected input to have only one field");
            }

            Schema.FieldSchema inputFieldSchema = input.getField(0);
            if (inputFieldSchema.type != DataType.CHARARRAY) {
                throw new RuntimeException("Expected a CHARARRAY as input");
            }

            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("itemName", DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("number", DataType.INTEGER));
            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), tupleSchema, DataType.BAG));
        } catch (FrontendException exx) {
            throw new RuntimeException(exx);
        }
    }
	
}
