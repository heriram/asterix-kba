package edu.uci.ics.asterix.external.library.textanalysis;

import edu.uci.ics.asterix.external.library.utils.StringUtil;

public class Tokenizer extends AbstractTokenizer implements ITokenizer {
    
    public static final Tokenizer INSTANCE = new Tokenizer();
    
    private Tokenizer() {
        
    }
    
    public String[] tokenize(char textCharArray[], boolean removeStopWord) {
        int len = textCharArray.length;
        String[] temp = new String[(len / 2) + 2];
        int wordCount = 0;
        char wordBuff[] = new char[len];
        int index = 0;

        for (int i = 0; i < len; i++) {
            char c = textCharArray[i];
            c = Character.toLowerCase(c);
            switch (c) {
                case '\'': // Remove "'s"
                    if (i==(len-1))
                        break;
                    
                    char next_c =  textCharArray[i+1];
                    if (next_c == 's') {
                        i++;
                    } else if (next_c == 't') { // keep 't forms for now
                        wordBuff[index] = c;
                        index++;
                        wordBuff[index] = next_c;
                        index++;
                        i++;
                    }
                    break;
                case ' ':
                case '\n':
                case '\t':
                case '\r':
                    if (index > 0) {
                        String word = new String(wordBuff, 0, index);
                        index = 0;
                        temp[wordCount] = word;
                        wordCount++;

                    }
                    break;

                case '&':
                case '@':
                case '-':
                case '_':
                    wordBuff[index] = c;
                    index++;

                    break;
                default:
                    if ((c >= toDigit(0) && c <= toDigit(9)) || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                            || (c >= 0x00C0 && c <= 0x00F6) || (c >= 0x00F8 && c <= 0x02AF)) {
                        wordBuff[index] = c;
                        index++;
                    }
            }

        }

        String lastToken = new String(wordBuff, 0, index).trim();
        if (!lastToken.isEmpty()) {
            temp[wordCount] = lastToken;
            wordCount++;
        }
        String result[] = new String[wordCount];
        System.arraycopy(temp, 0, result, 0, wordCount);
        
        if (removeStopWord)
            result = removeStopWord(result);

        return result;
    }

    @Override
    public String[] tokenize(String text, boolean removeStopWord) {
        return tokenize(text.trim().toCharArray(), removeStopWord);
    }

    @Override
    public String[] tokenize(String text) {
        String tokens[] = tokenize(text, true);
        return tokens;
    }

    public static void main(String[] args) {
        ITokenizer lexer = SMTokenizer.INSTANCE;
        String test = "@johnsmith: this is a test of this new tokenizer. It won't be bad if it works as good.";
        String text =  "Thermodynamic Geometry, Phase Transitions,"+
                "and the Widom Line"+
                "George Ruppeiner a ∗, Anurag Sahay b†, Tapobrata Sarkar b‡,"+
                "Gautam Sengupta b §"+
                "a Division of Natural Sciences,"+
                "New College of Florida, 5800 Bay Shore Road,"+
                "Sarasota, Florida 34243-2109"+
                "b Department of Physics,"+
                "Indian Institute of Technology,"+
                "Kanpur 208016,"+
                "India"+
                "Abstract"+
                "We construct a novel approach, based on thermodynamic geometry, to"+
                "characterize first-order phase transitions from a microscopic perspective,"+
                "through the scalar curvature in the equilibrium thermodynamic state space."+
                "Our method resolves key theoretical issues in macroscopic thermodynamic"+
                "constructs, and furthermore characterizes the Widom line through the"+
                "maxima of the correlation length, which is captured by the thermody-"+
                "namic scalar curvature. As an illustration of our method, we use it in"+
                "conjunction with the mean field Van der Waals equation of state to pre-"+
                "dict the coexistence curve and the Widom line. Where closely applicable,"+
                "it provides excellent agreement with experimental data. The universality"+
                "of our method is indicated by direct calculations from the NIST database."+
                "∗ ruppeiner@ncf.edu"+
                "†anurag@iopb.res.in, Current Address Institute of Physics, Bhubaneshwar, India"+
                "‡ tapo@iitk.ac.in"+
                "§ sengupta@iitk.ac.in"+
                "1"+
                "Macroscopic properties of matter undergo discontinuous changes along a first-"+
                "order liquid-gas coexistence curve that culminates in a critical point 1, and is"+
                "extendable into the supercritical region as the Widom line 2, 3, 4 charac-"+
                "terized by the locus of points with maximum correlation length ξ . Historically,"+
                "such phase coexistence curves were modeled by the van der Waals vdW equa-"+
                "tion augmented by the Maxwell “equal area” construction. This approach lies"+
                "at the foundation of the modern thermodynamic picture characterizing coexist-"+
                "ing phases through equal Gibbs free energies. However, the vdW-Maxwell theory"+
                "suffers from several unresolved conceptual drawbacks 5, 6, and furthermore, an"+
                "analytic prediction of the Widom line from any equation of state is yet unknown."+
                "Here we devise a novel construction to characterize liquid-gas phase transitions"+
                "based on the continuity of ξ between the phases, with the Riemannian geometric"+
                "thermodynamic scalar curvature R ∼ ξ 3 7, which also allows, for the first time,"+
                "a direct computation of the Widom line. The idea that the correlation lengths"+
                "of the coexisting phases are equal, and its computational realization through R,"+
                "provides a method for predicting the phase coexistence curve when used in con-"+
                "junction with any equation of state, or experimental data. We illustrate this point"+
                "here with the vdW equation, settling a century old problem of thermodynamic"+
                "computation. We also determine the location of the Widom line for several fluids"+
                "both with the vdW equation and with data from the NIST database 8. Our"+
                "results may be used to predict phase behaviour in a wide variety of systems, from"+
                "boiling water to black holes, and promises to have significant impact on diverse"+
                "areas of physical sciences and engineering."+
                "The key physical idea in our analysis originates from the microscopic picture"+
                "of first-order liquid-gas phase transitions due to Widom 9. In this framework,"+
                "spontaneous density fluctuations cause the local density ρ r in a single phase"+
                "fluid to deviate from the overall mean density ρ0 in some complex, time dependent"+
                "manner. Mathematically, ρ r = ρ0 corresponds to an intricate contour surface"+
                "that separates two sides with local mean densities ρ ρ0 and ρ ρ0 . A straight"+
                "line through the fluid intersects this surface at points spaced an average distance"+
                "ξ apart, where ξ is the correlation length characterizing the size of organized"+
                "structures inside the fluid. ξ is generally small in a disorganized system like an"+
                "ideal gas, but diverges at the critical point for real fluids. When a single phase"+
                "fluid is very near a first-order phase transition, a small amount of a second,"+
                "minority phase will begin to form. A reference point in this single phase fluid"+
                "typically has local density close to that of either of the two incipient coexisting"+
                "phases. The typical density difference ∆ρ across the contour surface ρ r ="+
                "ρ0 thus equals that of the two phases. Reversing the role of the ma jority and"+
                "minority phases leaves this argument unchanged, with the same ∆ρ. ξ in the"+
                "single phase plays a similar role in anticipating the properties in the two phases"+
                "since ξ is the thickness of the interface between the two 9. This anticipated"+
                "interface thickness must be the same approaching the phase transition with either"+
                "of the two phases being the ma jority phase, and the correlation length ξ should"+
                "thus be the same in the two coexisting phases, the condition at the heart of our"+
                "approach."+
                "For experimental predictability, we need an estimate of ξ , allowing a thermo-"+
                "dynamic expression for the equality of the correlation lengths at the interface."+
                "1"+
                "Figure 1"+
                "-R for the coexisting liquid and gas phases versus T − Tc Tc for"+
                "normal Hydrogen calculated with the NIST fluid database, plotted on the loga-"+
                "rithmic scale. At the indicated value, R ∼ vg , where vg is the molecular volume"+
                "in the gas phase. Below this value of R, its interpretation as the correlation"+
                "length loses significance."+
                "This can be realized using the Riemannian geometry of the equilibrium ther-"+
                "∂ 2 s"+
                "modynamic state space of the system through the metric gαβ = − 1"+
                "∂ aα ∂ aβ α,"+
                "kB"+
                "β = 1,2, where kB is Boltzmann’s constant, and s, a1 , and a2 denote the en-"+
                "tropy, energy, and particle number per unit volume, respectively 7. The metric"+
                "is based on Gaussian fluctuation theory whose breakdown takes place when the"+
                "volume of the system is of the order of the Riemann scalar curvature R of the"+
                "thermodynamic metric. This volume is expected to be the correlation volume"+
                "ξ 3 , leading to the desired connection 7, R ∼ ξ 3 . Experimental predictions"+
                "for the coexistence curves of first-order phase transitions can thus be obtained"+
                "from the equality of R calculated in the two coexisting phases. We call this the"+
                "R-crossing method. At the critical point, R diverges. In the supercritical region"+
                "beyond the critical point, the locus of the maximum of R, via R ∼ ξ 3 , provides"+
                "a theoretical prediction of the Widom line."+
                "As a direct test of our proposal, we calculate R for Hydrogen in both phases"+
                "using the NIST fluid database 8, 10 and its program REFPROP. These pro-"+
                "vide data based on phenomenological equations of state, with fit parameters"+
                "determined by matching to experimental data for fluids. Results are shown in"+
                "fig.1, where agreement between the R’s in the two phases is seen to be excellent"+
                "near the critical point, better than 1 in the range 0.96 T Tc 1, where T is"+
                "the temperature and Tc its critical value. By contrast, at T Tc = 0.96, the molar"+
                "densities of the coexisting liquid and gas phases differ from each other by a factor"+
                "of ∼ 3."+
                "Our R-crossing method complements the canonical macroscopic rule for first-"+
                "order phase transitions, namely the equality of the molar Gibb’s free energies g"+
                "of the coexisting phases 1. Applied to the vdW equation however, this macro-"+
                "2"+
                "Maxwell"+
                "R Crossing"+
                "0.2"+
                "0.3"+
                "0.4"+
                "0.5"+
                "0.6"+
                "0.7"+
                "Pr"+
                "R vapor"+
                "Pr Experiment = 0.55"+
                "Pr R Crossing = 0.56"+
                "Pr Maxwell = 0.53"+
                "R liquid"+
                "Vr"+
                "20"+
                "10"+
                "0"+
                "–10"+
                "–20"+
                "Rb"+
                "–30"+
                "Figure 2 R vs pr along an isotherm of Helium with tr = 0.86 in the lower half"+
                "and vr vs pr along the same tr in the upper half where the vr values have been"+
                "multiplied by a factor of 3. The blue and green curves represent the stable"+
                "branches, and the red curve is the unstable branch. We mark by “R-crossing”"+
                "the pr where the R’s of the liquid and gas phases become equal with cv = 1.5"+
                "and 1.2 on the gas and liquid sides respectively. The line labeled “Maxwell”"+
                "represents the corresponding pr from Maxwell’s construction."+
                "scopic equality has unresolved conceptual problems. Finding states with equal g ’s"+
                "involve contentious issues of integration along a reversible path through a thermo-"+
                "dynamically unstable region in the Maxwell construction, or through the critical"+
                "point in Kahl’s approach 11. Such conceptual difficulties, which have been de-"+
                "bated for over a century, are entirely bypassed in our construction. Further, our"+
                "method naturally contains a measure of its limit of applicability, since for the"+
                "construction to be effective, ξ 3 should be large enough to encompass a number"+
                "of atoms adequate for a thermodynamic approach to be reasonable. This limits"+
                "us to a regime not too far from the critical point. We find that the R-crossing"+
                "method retains its viability down to volume regimes containing but about a single"+
                "molecule."+
                "As a simple theoretical example, we apply the R-crossing method to the uni-"+
                "versal vdW equation in its reduced form,"+
                "pr ="+
                "8tr"+
                "3vr − 1"+
                "−"+
                "3"+
                "v 2"+
                "r"+
                ","+
                "1"+
                "where pr = P Pc , tr = T Tc , vr = vvc and P and v are the pressure and molar"+
                "volume, with the subscript c denoting their critical values. The critical quantities"+
                "27b2 , Tc = 8a"+
                "are known to be related to the vdW constants a and b by Pc = a"+
                "27kB T"+
                "and vc = 3b. The Maxwell equal-area construction yields the limiting slope of the"+
                "coexistence curve dpr dtr = 4, independent of the fluid and its heat capacity. Our"+
                "R-crossing method inherits the same limiting slope here. This number is closely"+
                "followed only by Helium and Hydrogen, for which this example is expected to"+
                "3"+
                "Figure 3 Phase coexistence and the Widom line for Helium Tc = 5.19K, Pc ="+
                "2.26 bars on the left and Hydrogen Tc = 33.19K, Pc = 13.30bars on the"+
                "right. The coexistence curve is calculated from vdW with the Maxwell equal-area"+
                "construction and with R-crossing, and compared with experimental data from"+
                "NIST 8. The Widom line is calculated by finding the locus of maximum values"+
                "of R both with vdW and from NIST data. We compare with the maximum"+
                "values of cp from experimental data NIST. The liquid and gas heat capacities"+
                "cvl and cvg are indicated for vdW. In the supercritical region, we use cvg"+
                "be maximally effective. R can be calculated here via the thermodynamic metric"+
                "using standard formulae 7 and gives R = A · B , where"+
                "A = −"+
                "b"+
                "3"+
                "3vr − 1"+
                "r − 3vr 22 ,"+
                "cv pr v 3"+
                "and"+
                "r − 9pr v 4"+
                "B = cv cid0p2"+
                "r − pr v 2"+
                "r − 27v 2"+
                "r 12pr v 3"+
                "r v 5"+
                "r 27vr − 3cid1"+
                "18vr cid0pr v 3"+
                "r 1cid1"+
                "2"+
                "3"+
                "where cv is the dimensionless molecular specific heat at constant volume assumed"+
                "constant, though possibly different in the liquid and gas phases and b plays no"+
                "role in our subsequent analysis."+
                "For vdW isotherms with given tr 1, substituting pr from eq.1 into eqs.2"+
                "and 3 results in two physical branches for R, corresponding to the liquid and"+
                "gas phases see color coded fig.2, with R diverging at the end points. The value"+
                "of pr where the R values are equal i.e they cross is interpreted as the reduced"+
                "saturation pressure corresponding to tr . For the cases we consider here, cv on the"+
                "gas side is taken as 1.5, the ideal gas value. On the liquid side, we have chosen"+
                "average values determined from NIST data 8, over the range of temperatures"+
                "that we are interested in. Equivalently, for vdW isobars, the R-crossing method"+
                "can be used to predict the saturation temperature."+
                "In the supercritical region, isobaric R exhibits a local maximum with respect"+
                "to tr , whose locus is naturally interpreted as the Widom line, signifying a crossover"+
                "for certain dynamical fluid properties from gas like on the low pressure side to"+
                "4"+
                "P Pc T sat"+
                "R vdW"+
                "0.4"+
                "36.04"+
                "37.68"+
                "0.5"+
                "39.24"+
                "0.6"+
                "42.09"+
                "0.8"+
                "0.9"+
                "43.33"+
                "122.89"+
                "0.4"+
                "128.19"+
                "0.5"+
                "0.6"+
                "133.34"+
                "142.70"+
                "0.8"+
                "0.9"+
                "146.85"+
                "T sat"+
                "ex"+
                "37.97"+
                "39.41"+
                "40.66"+
                "42.76"+
                "43.66"+
                "129.16"+
                "133.93"+
                "138.07"+
                "145.00"+
                "147.98"+
                "Rvg"+
                "0.60"+
                "1.16"+
                "2.36"+
                "15.35"+
                "76.48"+
                "0.57"+
                "1.11"+
                "2.23"+
                "14.29"+
                "72.49"+
                "R vdW T W"+
                "P Pc T W"+
                "R NIST"+
                "1.1"+
                "45.56"+
                "45.25"+
                "45.95"+
                "46.57"+
                "1.2"+
                "47.26"+
                "48.43"+
                "1.4"+
                "48.50"+
                "50.15"+
                "1.6"+
                "2.0"+
                "53.26"+
                "50.83"+
                "153.15"+
                "154.32"+
                "1.1"+
                "155.47"+
                "157.74"+
                "1.2"+
                "1.4"+
                "164.04"+
                "159.72"+
                "163.69"+
                "169.84"+
                "1.6"+
                "2.0"+
                "180.41"+
                "170.96"+
                "T W"+
                "ex"+
                "45.26"+
                "46.01"+
                "47.39"+
                "48.64"+
                "50.79"+
                "153.21"+
                "155.60"+
                "160.00"+
                "163.89"+
                "170.49"+
                "Table 1 Saturation temperatures on the left and Widom line temperatures on"+
                "the right in Kelvins for Neon Tc = 44.49 K, Pc = 26.79 bars on the upper"+
                "part and Argon Tc = 150.69 K, Pc = 48.63 bars on the lower part. T sat"+
                "R vdW"+
                "is the prediction of the saturation temperature from the R-crossing method, us-"+
                "ing the vdW equation, and is compared with experimental values from NIST."+
                "Corresponding values of Rvg are also shown to indicate the validity of our"+
                "method. Widom line predictions from the R-maximization method are obtained"+
                "both from vdW with cv = 1.5 T W"+
                "R vdW, and from NIST T W"+
                "R NIST. We"+
                "have also shown the prediction of the Widom line obtained from maximising cp"+
                "as T W"+
                "ex ."+
                "liquid like on the high pressure side 2, 3, 4. We can calculate the Widom"+
                "line as per its definition through R ∼ ξ 3 , free from the theoretical difficulty of"+
                "characterizing it via the maximum of the specific heat cp as is conventional in the"+
                "literature 3. To find maxima for R and cp , we search along isobars."+
                "A natural estimate for the validity of our analysis for vdW is offered by the"+
                "dimensionless quantity Rvg , where vg is the coexistence molecular volume in"+
                "the gas phase. R ≥ vg implies that ξ 3 is greater than a molecular volume, and"+
                "we are in a regime where our analysis is strictly valid. We find that for vdW, this"+
                "restricts us to tr & 0.8 along the coexistence curve a value indicated in fig.3"+
                "and table 1, and to pr . 10, in the supercritical region."+
                "Figure 3 summarizes our results for Helium and Hydrogen. Table 1 sup-"+
                "plements these for Neon and Argon. From fig.3, it can be seen that the R-"+
                "crossing method, in conjunction with vdW, predicts excellent results within its"+
                "range of applicability. Away from criticality, deviation from data is also due to"+
                "the mean field nature of the vdW equation of state. Direct application of the"+
                "R-maximization method using NIST data in the supercritical regime shows strik-"+
                "ing agreement with experimental cp maximum values even far from the scaling"+
                "region."+
                "In conclusion, we have constructed a novel geometrical technique to charac-"+
                "terize liquid-gas phase transitions from a microscopic perspective, through the"+
                "thermodynamic scalar curvature R. When applied in conjunction with the vdW"+
                "equation, this bypasses theoretical issues a century old with the Maxwell equal"+
                "area construction and its variants. Our technique generalizes to any phenomeno-"+
                "5"+
                "logical equation of state, including those obtained as multi parameter fits to"+
                "experimental data. This analysis further provides the first direct theoretical con-"+
                "struction of the Widom line, without using any ad hoc thermodynamic response"+
                "function."+
                "Our method unifies concepts in Riemannian geometry, thermodynamics, phase"+
                "transitions, critical and supercritical phenomena. Although we have primarily"+
                "applied our technique to liquid-gas phase transitions, the method should be uni-"+
                "versally applicable to any first-order phase transition. This makes it of crucial"+
                "significance to a diverse range of disciplines in physical, chemical and biological"+
                "sciences, and engineering. It further generalises even to gravitational systems like"+
                "anti de-Sitter black holes which also appear to exhibit liquid-gas like first-order"+
                "phase transitions 12."+
                "Acknowledgements"+
                "We thank Steven Shipman and Helge May for valuable input, and Eric Lemmon"+
                "for programming R into REFPROP 9.01, allowing us to verify the numbers in"+
                "Figure 1 and readily compute Widom lines with R. TS thanks the Saha Institute"+
                "of Nuclear Physics, Kolkata, India for its hospitality where a part of this work"+
                "was completed."+
                "References"+
                "1 Callen, H. B. Thermodynamics and an Introduction to Thermostatistics,"+
                "John Wiley & Sons, New York, 1985."+
                "2 Widom, B. in Phase Transitions and Critical Phenomena, Vol. 2 eds Domb,"+
                "C. & Green, M. S. Academic Press, 1972."+
                "3 McMillan, P. F. & Stanley, H. E. Fluid Phases Going supercritical, Nature"+
                "Physics 6, 479 2010."+
                "4 Simeoni, G. G. et al. The Widom line as the crossover between liquid-like"+
                "and gas-like behaviour in supercritical fluids. Nature Physics 6, 503 2010."+
                "5 Tisza, L. Generalized Thermodynamics, M.I.T Press, Cambridge, Mass,"+
                "1966."+
                "6 Pippard, A. B. The Elements of Classical Thermodynamics, Cambridge Uni-"+
                "versity Press, 1964."+
                "7 Ruppeiner, G. Riemannian geometry in thermodynamic fluctuation theory."+
                "Rev. Mod. Phys. 67, 605 1995, erratum ibid 68, 313 1996."+
                "8 NIST Chemistry WebBook, available at the Web Site"+
                "httpwebbook.nist.govchemistry ."+
                "9 Widom, B. The critical point and scaling theory. Physica 73, 107 1974."+
                "6"+
                "10 Leachman, J. W., Jacobson, R. T., Penoncello, S. G. & Lemmon, E. W."+
                "Fundamental Equations of State for Parahydrogen, Normal Hydrogen and"+
                "Orthohydrogen. J. Phys. Chem. Ref. Data 38, 721 2009."+
                "11 Kahl, G. D. Generalization of the Maxwell criterion for Van der Waals equa-"+
                "tion. Phys. Rev. 155, 78 1967."+
                "12 Sahay, A., Sarkar, T. & Sengupta, G. Thermodynamic geometry and phase "+
                "transitions in Kerr-Newman- AdS Black Holes. Journal of High Energy "+
                "Physics 1004 118 2010.7 ";
        String tokens[] = lexer.tokenize(text);        
        System.out.println(StringUtil.concatenate(tokens, '|'));
    }

}
