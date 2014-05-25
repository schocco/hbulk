package load;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;

import org.apache.james.mime4j.MimeException;
import org.apache.james.mime4j.stream.EntityState;
import org.apache.james.mime4j.stream.Field;
import org.apache.james.mime4j.stream.MimeTokenStream;

/**
 * An email representation.
 */
final class Email {
	
	/** The stream. */
	private MimeTokenStream stream = new MimeTokenStream();
	
	/** The headers. */
	private HashMap<String, String> headers = new HashMap<>();
	
	/** The body. */
	private String body;
	
	/** The body meta. */
	private String bodyMeta;

	/**
	 * Parses the message and stores headers and body in attributes.
	 *
	 * @param message the MIME email message
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws MimeException the mime exception
	 */
	public Email(byte[] message) throws IOException, MimeException {
		stream.parse(new ByteArrayInputStream(message));
		for (EntityState state = stream.getState(); state != EntityState.T_END_OF_STREAM; state = stream
				.next()) {
			switch (state) {
			case T_BODY:
				body = stream.getInputStream().toString();
				bodyMeta = stream.getBodyDescriptor().toString();
				break;
			case T_FIELD:
				Field field = stream.getField();
				if(field.getBody() != null &! field.getBody().isEmpty()){
					headers.put(field.getName(), field.getBody());
				}					
				break;
			default:
				break;
			}
		}
	}
	
	/**
	 * Gets the headers.
	 *
	 * @return the headers
	 */
	public HashMap<String, String> getHeaders() {
		return headers;
	}

	/**
	 * Gets the body.
	 *
	 * @return the body
	 */
	public String getBody() {
		return body;
	}

	/**
	 * Gets the body meta.
	 *
	 * @return the body meta
	 */
	public String getBodyMeta() {
		return bodyMeta;
	}
	
	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public String getId(){
		return headers.get("Message-ID");
	}
}