import java.io.File;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.formats.RDFXMLDocumentFormat;

public class ConvertToRDFXML {
    public static void main(String[] args) throws Exception {
        File in = new File(args[0]);
        File out = new File(args[1]);
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(in);
        manager.saveOntology(ontology, new RDFXMLDocumentFormat(), IRI.create(out));
        System.out.println("Converted: " + ontology.getAxiomCount() + " axioms");
    }
}
