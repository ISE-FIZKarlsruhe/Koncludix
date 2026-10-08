import java.io.File;
import java.util.*;
import java.util.stream.*;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;

/**
 * Lint report for an ontology that is going to be given to Konclude: lists constructs and data patterns that are known to be expensive for
 * (this build of) Konclude or that it does not handle completely, with example axioms and advice.
 *   java -cp tools/openllet.jar:tools KoncludeLint ontology.owl
 */
public class KoncludeLint {
	static int warnings = 0;
	static void warn(String title, String detail, List<String> examples) {
		++warnings;
		System.out.println("[WARN] " + title);
		System.out.println("       " + detail);
		for (String e : examples.stream().limit(3).collect(Collectors.toList())) System.out.println("       e.g. " + (e.length() > 170 ? e.substring(0, 170) + "..." : e));
	}
	static void info(String t) { System.out.println("[INFO] " + t); }
	static String sh(String iri) { int i = Math.max(iri.lastIndexOf('#'), iri.lastIndexOf('/')); return i >= 0 ? iri.substring(i + 1) : iri; }

	public static void main(String[] a) throws Exception {
		OWLOntologyManager m = OWLManager.createOWLOntologyManager();
		m.getOntologyConfigurator().setMissingImportHandlingStrategy(MissingImportHandlingStrategy.SILENT);
		OWLOntology o = m.loadOntologyFromOntologyDocument(new File(a[0]));
		long classAss = o.axioms(AxiomType.CLASS_ASSERTION).count(), objAss = o.axioms(AxiomType.OBJECT_PROPERTY_ASSERTION).count(), dataAss = o.axioms(AxiomType.DATA_PROPERTY_ASSERTION).count();
		long inds = o.individualsInSignature().count();
		System.out.println("Ontology: " + a[0]);
		System.out.println("Axioms " + o.getAxiomCount() + " | classes " + o.classesInSignature().count() + ", object properties " + o.objectPropertiesInSignature().count() + ", data properties " + o.dataPropertiesInSignature().count()
				+ " | individuals " + inds + ", class assertions " + classAss + ", object property assertions " + objAss + ", data property assertions " + dataAss);

		List<String> hasKey = new ArrayList<>(), ifp = new ArrayList<>(), qcard = new ArrayList<>(), wideOneOf = new ArrayList<>(), wideUnion = new ArrayList<>(), bigCard = new ArrayList<>(), complexGci = new ArrayList<>();
		List<String> minCardN = new ArrayList<>(), hasValue = new ArrayList<>(), selfR = new ArrayList<>();
		Set<String> transitive = new HashSet<>(), symmetric = new HashSet<>();
		boolean nominals = false, inverse = false, anyCard = false;
		int maxCard = 0;
		for (OWLAxiom ax : (Iterable<OWLAxiom>) o.axioms().filter(x -> x.isLogicalAxiom())::iterator) {
			if (ax instanceof OWLHasKeyAxiom) hasKey.add(ax.toString());
			if (ax instanceof OWLInverseFunctionalObjectPropertyAxiom) ifp.add(ax.toString());
			if (ax instanceof OWLTransitiveObjectPropertyAxiom) transitive.add(((OWLTransitiveObjectPropertyAxiom) ax).getProperty().getNamedProperty().getIRI().toString());
			if (ax instanceof OWLSymmetricObjectPropertyAxiom) symmetric.add(((OWLSymmetricObjectPropertyAxiom) ax).getProperty().getNamedProperty().getIRI().toString());
			if (ax instanceof OWLInverseObjectPropertiesAxiom) inverse = true;
			if (ax instanceof OWLDisjointUnionAxiom && ((OWLDisjointUnionAxiom) ax).getClassExpressions().size() >= 8) wideUnion.add(ax.toString());
			if (ax instanceof OWLSubClassOfAxiom) {
				OWLClassExpression sub = ((OWLSubClassOfAxiom) ax).getSubClass();
				if (sub.isAnonymous()) {
					boolean simple = sub instanceof OWLObjectIntersectionOf && sub.asConjunctSet().stream().allMatch(c -> !c.isAnonymous() || c instanceof OWLObjectSomeValuesFrom);
					if (!simple && !(sub instanceof OWLObjectSomeValuesFrom)) complexGci.add(ax.toString());
				}
			}
			ax.nestedClassExpressions().forEach(ce -> {});
			for (OWLClassExpression ce : (Iterable<OWLClassExpression>) ax.nestedClassExpressions()::iterator) {
				if (ce instanceof OWLObjectOneOf) { /* handled below */ }
			}
		}
		for (OWLAxiom ax : (Iterable<OWLAxiom>) o.axioms().filter(x -> x.isLogicalAxiom())::iterator) {
			for (OWLClassExpression ce : (Iterable<OWLClassExpression>) ax.nestedClassExpressions()::iterator) {
				if (ce instanceof OWLObjectOneOf) { nominals = true; int k = ((OWLObjectOneOf) ce).individuals().toArray().length; if (k >= 10) wideOneOf.add(k + " nominals in " + ax); }
				if (ce instanceof OWLObjectHasValue) { nominals = true; hasValue.add(ax.toString()); }
				if (ce instanceof OWLObjectUnionOf && ((OWLObjectUnionOf) ce).getOperandsAsList().size() >= 8) wideUnion.add(((OWLObjectUnionOf) ce).getOperandsAsList().size() + " disjuncts in " + ax);
				if (ce instanceof OWLObjectHasSelf) selfR.add(ax.toString());
				if (ce instanceof OWLObjectCardinalityRestriction) {
					OWLObjectCardinalityRestriction cr = (OWLObjectCardinalityRestriction) ce;
					anyCard = true; maxCard = Math.max(maxCard, cr.getCardinality());
					if (cr.getCardinality() >= 10) bigCard.add(cr.getCardinality() + " in " + ax);
					if (!cr.getFiller().isOWLThing()) qcard.add(ax.toString());
					if (ce instanceof OWLObjectMinCardinality && cr.getCardinality() >= 2) minCardN.add(ax.toString());
				}
				if (ce instanceof OWLObjectPropertyExpression) {}
			}
		}
		info("Features: nominals " + nominals + ", inverse roles " + inverse + ", number restrictions " + anyCard + (anyCard ? " (largest constant " + maxCard + ")" : "") + ", transitive roles " + transitive.size() + ", hasKey axioms " + hasKey.size());

		if (!hasKey.isEmpty()) warn("hasKey is not enforced", "Konclude (this build) does not derive sameAs or inconsistencies from owl:hasKey, so a violated key goes unnoticed (InferTest: has-key fails).", hasKey);
		if (!ifp.isEmpty()) warn("InverseFunctionalProperty is not enforced for merging", "Two individuals with the same inverse-functional successor are not merged (InferTest: inverse-functional-property fails).", ifp);
		if (!qcard.isEmpty()) warn("Qualified cardinality restrictions", "Qualified (filler other than owl:Thing) number restrictions on asserted individuals are not used to merge/clash individuals (InferTest: qualified-cardinality fails); they also create successors for every individual.", qcard);
		if (!wideOneOf.isEmpty()) warn("Large nominal enumerations", "Every node that must belong to an enumeration of n nominals is a n-way decision that stays live on the search path; with thousands of such nodes this dominates memory (OWL2Bench pattern: 28 interests).", wideOneOf);
		if (!wideUnion.isEmpty()) warn("Wide disjunctions", "Disjunctions or disjoint unions with 8 or more operands create one task per alternative at every node they apply to.", wideUnion);
		if (!bigCard.isEmpty()) warn("Large cardinality constants", "Minimum cardinality restrictions create that many distinct successors per individual (unless asserted neighbours already satisfy them).", bigCard);
		if (!complexGci.isEmpty()) warn("General concept inclusions with a complex left-hand side", complexGci.size() + " axiom(s) that probably cannot be absorbed; each is applied to every individual as a disjunction.", complexGci);
		if (!selfR.isEmpty() || !hasValue.isEmpty() && !hasKey.isEmpty()) warn("Combination of nominals/hasValue with hasKey/Self", "The verdict was observed to depend on the axiom order for such combinations; test with a second ordering if the result matters.", hasValue);

		// ABox based checks
		Map<String, List<String[]>> edgesByRole = new HashMap<>();
		o.axioms(AxiomType.OBJECT_PROPERTY_ASSERTION).forEach(pa -> {
			if (pa.getSubject().isNamed() && pa.getObject().isNamed() && !pa.getProperty().isAnonymous())
				edgesByRole.computeIfAbsent(pa.getProperty().asOWLObjectProperty().getIRI().toString(), k -> new ArrayList<>()).add(new String[]{pa.getSubject().asOWLNamedIndividual().getIRI().toString(), pa.getObject().asOWLNamedIndividual().getIRI().toString()});
		});
		for (String r : transitive) {
			List<String[]> es = edgesByRole.getOrDefault(r, Collections.emptyList());
			if (es.isEmpty()) continue;
			Map<String, String> parent = new HashMap<>();
			java.util.function.Function<String, String> find = new java.util.function.Function<String, String>() {
				public String apply(String x) { parent.putIfAbsent(x, x); String p = x; while (!parent.get(p).equals(p)) p = parent.get(p); String c = x; while (!parent.get(c).equals(p)) { String n = parent.get(c); parent.put(c, p); c = n; } return p; } };
			boolean selfLoop = false;
			for (String[] e : es) { String x = find.apply(e[0]), y = find.apply(e[1]); if (!x.equals(y)) parent.put(x, y); if (e[0].equals(e[1])) selfLoop = true; }
			Map<String, Integer> sizes = new HashMap<>();
			for (String k : new ArrayList<>(parent.keySet())) sizes.merge(find.apply(k), 1, Integer::sum);
			long pairs = 0; for (int sz : sizes.values()) pairs += (long) sz * sz;
			boolean sym = symmetric.contains(r);
			if (sym && pairs > 100000) warn("Transitive and symmetric role '" + sh(r) + "' with a large ABox", "Its closure is quadratic in the size of each connected group: about " + pairs + " role assertions would be materialized (about 1 ms per 1000 assertions, output file size accordingly).", Collections.singletonList(sh(r) + ": " + es.size() + " asserted edges in " + sizes.size() + " groups"));
			else if (!sym && pairs > 1000000) info("Transitive role '" + sh(r) + "': closure of up to about " + pairs + " assertions possible");
			// cycles (directed) via self loops or mutual edges
			Set<String> seen = new HashSet<>(); boolean cycle = selfLoop;
			for (String[] e : es) if (seen.contains(e[1] + ">" + e[0])) cycle = true; else seen.add(e[0] + ">" + e[1]);
			if (cycle) warn("Cycle or self loop on transitive role '" + sh(r) + "' in the ABox", "With this build, materialize can give non-deterministic output (an asserted triple of the cycle is sometimes missing) and in rare cases hangs.", Collections.singletonList(sh(r) + (selfLoop ? " has a self loop" : " has mutual edges")));
		}
		if (inds > 5000) warn("Large ABox (" + inds + " individuals)", "Use one worker (-w 1): with several workers idle workers explore sibling alternatives speculatively and memory grows several times (DL-5: 2.1 GB with -w 1, over 10 GB with -w 4). The minimum-cardinality optimisation (AtLeastBackendNeighbourSatisfaction) is on by default.", Collections.emptyList());
		if (inds > 1000 && !minCardN.isEmpty()) info("Minimum cardinality restrictions with n >= 2 on a large ABox: asserted neighbours are counted instead of creating new successors (option AtLeastBackendNeighbourSatisfaction, default on).");
		if (inds > 1000) {
			try {
				AboxReduce.Model M = AboxReduce.analyse(a[0]);
				Map<Integer, Integer> sizes = new HashMap<>();
				for (String i : M.inds) if (!M.pinned.contains(i)) sizes.merge(M.color.get(i), 1, Integer::sum);
				int removable = 0; for (int sz : sizes.values()) removable += sz - 1;
				info("ABox reduction: " + removable + " of " + M.inds.size() + " individuals (" + (100 * removable / Math.max(1, M.inds.size())) + " %) are interchangeable with another one"
						+ (removable * 5 >= M.inds.size() ? " -> tools/materialize-reduced.sh is worth trying." : " -> little to gain (run 'AboxReduce report' for the reasons)."));
			} catch (Throwable t) { info("ABox reduction estimate not available: " + t); }
		}
		System.out.println(warnings == 0 ? "No problems found." : warnings + " warning(s).");
	}
}
