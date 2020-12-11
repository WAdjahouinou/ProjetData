package fr.project.info.functions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Builder
@AllArgsConstructor
@Data
public class Record implements Serializable {
    private String libelleCommune;
    private String montantTotal;


}
