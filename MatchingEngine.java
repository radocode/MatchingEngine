package cl;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

@Singleton
public class MatchingEngine {

    private void MatchingEngine() {
        //no inicializar
    }

    @PersistenceContext(unitName = "cl.core_REST_war_0.1-SNAPSHOTPU")
    private EntityManager em;

    @EJB
    private BookingFacadeLocal bookingFacade;

    @EJB
    private ParamsFacadeLocal paramsFacade;

    @Schedule(minute = "*", hour = "*")
    private void match() {

        LocalDateTime matchingDateStart = LocalDateTime.now();

        Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "Transforma los bookings rejected en nuevos. Comprobando...");

        //toma los rejected y los transforma a nuevos
        List<Booking> bookingsRejected = em.createQuery("SELECT o FROM Booking o WHERE o.bookingState = :state")
                .setParameter("state", Constants.BOOKING_STATE_REJECTED).getResultList();

        for (Booking rejected : bookingsRejected) {
            //cambia estado a nuevo
            rejected.setBookingState(Constants.BOOKING_STATE_CREATED);

            //TODO: crear tabla de Booking Reject que tenga información de rechazos
            //TODO: agrega profesional a lista de rechazos
            rejected.getUserProfessional().getUser1().setRejectedWorks(rejected.getUserProfessional().getUser1().getRejectedWorks() + "," + rejected.getBookingId());

            //quita profesional
            rejected.setUserProfessional(null);
            //graba booking
            bookingFacade.edit(rejected);
        }
        // PRUEBA: Debemos tener dos profesionales en la base de datos y rechazar con uno de ellos para probar

        Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "Busca nuevos bookings para ser asignados. Comprobando...");

        //---------------------------------------------
        //busca nuevos para ser asignados
        List<Booking> bookings = em.createQuery("SELECT o FROM Booking o WHERE o.bookingState = :state")
                .setParameter("state", Constants.BOOKING_STATE_CREATED).getResultList();
        for (Booking booking : bookings) {

            Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "Se comprueba de que hayan bookings disponibles. Comprobando booking id: {0}", new Object[]{booking.getBookingId()});

            //traer pros con tipo de booking, ordenados por score
            //TODO: de la ciudad
            List<Professional> pros = em.createQuery("SELECT o FROM Professional o WHERE o.professionalType = :bookingType AND o.active = TRUE ORDER BY o.score DESC")
                    .setParameter("bookingType", booking.getUserProfessionalRole()).getResultList();
            for (Professional pro : pros) {
                Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "Se comprueba de que hayan profesionales disponibles. Comprobando profesional: {0} activo: {1} rol: {2}", new Object[]{pro.getUser(), pro.getActive(), pro.getProfessionalType().getUserRole()});

                //traer profesionales con availability en sus schedules
                //parsear json para saber duración, con fecha inicio y fecha fin calculada hacer un between
                //del json hay que obtener lo siguiente: selectedRooms y selectedBathrooms. la suma de ellos
                //se multiplica por la unidad minima (30 minutos, o 0.5 horas) y luego se guarda para el conteo
                BookingData data = Utils.storedBookingDataFromJson(booking.getBookingFields());

                //se asume el tiempo en minutos
                int selectedRoomsTime = 0;
                int selectedBathroomsTime = 0;
                int selectedExtras = 0;

                if (data != null) {
                    if (data.getSelectedRooms() != null) {
                        selectedRoomsTime = data.getSelectedRooms();
                    }
                    if (data.getSelectedBathrooms() != null) {
                        selectedBathroomsTime = data.getSelectedBathrooms();
                    }

                    if (data.getExtras() != null) {
                        for (Extra extra : data.getExtras()) {
                            if (extra.getChecked()) {
                                //contar los extras como media hora cada uno
                                selectedExtras = selectedExtras + 1;
                            }
                        }
                    }
                }

                //en la query de abajo, la duración del json debería ser igual a la contada en la query
                Date bookingEndTime = new Date(booking.getBookingTime().getTime());
                bookingEndTime.setMinutes(bookingEndTime.getMinutes() + ((selectedRoomsTime + selectedBathroomsTime + selectedExtras) * 30));
                long countAvailability = (Long) em.createQuery("SELECT count(o) FROM WorkAvailability o WHERE o.Professional = :pro AND o.availabilityHour BETWEEN :startTime AND :endTime ")
                        .setParameter("pro", pro).setParameter("startTime", booking.getBookingTime()).setParameter("endTime", bookingEndTime).getSingleResult();

                //ya que ambos estan medidos en unidades de 30, pues solo se debe sumar las unidades y comparar
                if (selectedRoomsTime + selectedBathroomsTime + selectedExtras == countAvailability) {
                    Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "Booking supera al tiempo disponible de este professional. Email: {0}", pro.getUser());

                    continue;
                }

                //TODO: filtrar profesionales que no tienen schedule, eso se sabe si el countSchedule da mayor a 0 usando el margen
                //parametrizado (1 hora) para el between dentro de la misma query
                int timeBetweenBookings = Integer.parseInt(paramsFacade.find(ParamKeys.WORK_TIME_BETWEEN_BOOKINGS).getParamValue());
                Date bookingEndTimeSchedule = new Date(booking.getBookingTime().getTime());
                bookingEndTime.setMinutes(bookingEndTime.getMinutes() + ((selectedRoomsTime + selectedBathroomsTime + selectedExtras) * 30) + timeBetweenBookings);
                Date bookingStartTimeSchedule = new Date(booking.getBookingTime().getTime());
                bookingStartTimeSchedule.setMinutes(bookingStartTimeSchedule.getMinutes() - timeBetweenBookings);
                long countSchedule = (Long) em.createQuery("SELECT count(o) FROM WorkSchedule o WHERE o.Professional = :pro AND o.scheduleTimestamp BETWEEN :dateStartHour AND :dateEndHour")
                        .setParameter("pro", pro).setParameter("dateStartHour", bookingStartTimeSchedule).setParameter("dateEndHour", bookingEndTimeSchedule).getSingleResult();

                if (countSchedule != 0) {
                    Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "Professional copado de trabajo , no disponible. Email: {0}", pro.getUser());

                    continue;
                }

                //filtrar profesionales que rechazaron anteriormente la tarea
                //hacer split en campo de rechazos y ver si acaso el booking id está ahí, si está entonces ignorar profesional
                if (pro.getUser1().getRejectedWorks() != null && Arrays.asList(pro.getUser1().getRejectedWorks().split(",")).contains(booking.getBookingId() + "")) {
                    Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "Booking rechazado, no elegible. Email: {0} Booking id: {1}", new Object[]{pro.getUser(), booking.getBookingId()});

                    continue;
                }
                //asignar al primero de los restantes, realizar push y pasar al siguiente booking
                booking.setUserProfessional(pro);
                pushBookingState(paramsFacade.find(ParamKeys.PUSH_TITLE).getParamValue(), booking.getUserProfessional().getUser1(), booking);
                break;
            }

            if (booking.getUserProfessional() == null) {
                //TODO: si no hay profesionales enviar correo al COO y poner en estado especial el booking
                //cambiar a un estado especial para que no salga siempre
                booking.setBookingState(Constants.BOOKING_STATE_UNASSIGNED);
                bookingFacade.edit(booking);

                //se manda correo a COO
                Map<String, String> vars = new HashMap<>();
                vars.put("DETAILS", "No hay profesionales disponibles.");

                //Email.getEmail(paramsFacade).sendEmail("profesionales-no-disponibles", paramsFacade.findPublicKey("EMAIL_COO").getParamValue(), vars);
                Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "No hay professionales disponibles, se manda correo a COO");

            }
        }

        //---------------------------------------------
        //revisa propuestos para cancelarlos si no respondieron
        List<Booking> bookingsProposed = em.createQuery("SELECT o FROM Booking o WHERE o.bookingState = :state")
                .setParameter("state", Constants.BOOKING_STATE_PROPOSED).getResultList();
        for (Booking proposed : bookingsProposed) {

            Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "Buscando bookings para ser propuestos. Comprobando booking id: {0}", proposed.getBookingId());

            if (proposed.getBookingStatusTime() != null) {
                Date proposedTime = new Date(proposed.getBookingStatusTime().getTime());

                proposedTime.setSeconds(proposedTime.getSeconds() + Integer.parseInt(paramsFacade.find(ParamKeys.PUSH_TIME_SUGGESTED_BOOKING).getParamValue()));

                Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "Booking se cambiará de estado de propuesto a cancelado por timeout. Booking id: {0} Date para el rechazo: {1}", new Object[]{proposed.getBookingId(), proposedTime});

                if (proposedTime.after(new Date())) {
                    //cambiar estado a rechazado
                    proposed.setBookingState(Constants.BOOKING_STATE_REJECTED);
                    //TODO: hay que agregarlos a la lista de rejecteds o en su defecto,a una tabla de rejecteds

                    //enviar push indicando eso al front
                    pushBookingState(paramsFacade.find(ParamKeys.PUSH_TITLE_CANCELED).getParamValue(), proposed.getUserProfessional().getUser1(), proposed);
                    //graba booking
                    bookingFacade.edit(proposed);

                    Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "Booking se cambia de estado de propuesto a cancelado por timeout. Booking id: {0}", proposed.getBookingId());
                }
            }
        }
        // PRUEBA: Esperar que un booking se cancele solo

        LocalDateTime matchingDateEnd = LocalDateTime.now();

        long diffInMilli = java.time.Duration.between(matchingDateStart, matchingDateEnd)
                .toMillis();

        Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "Revisando match {0} se ha demorado: {1} milisegundos.", new Object[]{LocalDateTime.now(), diffInMilli});
    }

    private void pushBookingState(String statusMsg, User user, Booking booking) {
        String urlParametersCanceled = Utils.createIonicJson(user.getTokensIonic(), statusMsg, booking.getBookingId(), booking.getBookingState());
        Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "Se prepara post a Ionic Push con los parametros: urlParametersCanceled: {0} ionic app id: {1} y tokenClienteIonic: {2}", new Object[]{urlParametersCanceled, paramsFacade.find(ParamKeys.IONIC_APP_ID).getParamValue(), user.getTokensIonic()});
        String postResultCanceled = Utils.executeIonicPost(paramsFacade.find(ParamKeys.IONIC_SERVICE_URL).getParamValue(), urlParametersCanceled, paramsFacade.find(ParamKeys.IONIC_APP_ID).getParamValue(), paramsFacade.find(ParamKeys.IONIC_SECRET_KEY).getParamValue());
        Logger.getLogger(MatchingEngine.class.getName()).log(Level.INFO, "Se manda post a Ionic Push, booking id: {0}, result: {1}", new Object[]{booking.getBookingId(), postResultCanceled});
    }
}
