package shared.messages;



public interface SerializeDeserializable {
    
    public String serialize(); 

    public void deserialize(String jsonData); 

}