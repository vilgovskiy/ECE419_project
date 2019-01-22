package shared.communication;

import shared.messages.TextMessage;

import java.io.IOException;


public interface CommunicationInterface {
    
    public void sendMessage(TextMessage msg) throws IOException;

    public TextMessage receiveMessage() throws IOException;

}