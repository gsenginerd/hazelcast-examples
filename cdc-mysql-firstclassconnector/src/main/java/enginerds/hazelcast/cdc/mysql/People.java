/**
 *
 */
package enginerds.hazelcast.cdc.mysql;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author SGopala
 *
 */
public class People implements Serializable {

	@JsonProperty("id")
	public int id;

	@JsonProperty("name")
	public String name;

	public People() {
	}
}
