Para este trabajo no se exige redactar un informe, pero pueden documentarse decisiones de diseño en este archivo.

---

Para el trabajo decidí implementar el protocol del middleware a través de golang.
Esto me llevó a tener que crear un struct de la working queue de rabbit que sepa responder los mismos mensajes que me impone el middleware y devolver luego en la factory ese struct que ya sabe implementar esos mismos mensajes.

## Work Queue

Algo que me resultó confuso fue que la documentación de rabbit habla de que en la WQ un productor envía mensajes a la cola pero en realidad de fondo esta interacción no ocurre y siempre que se publican mensajes, se hace a través de un exchange.

Para la WC, creo una queue y utilizo el exchange por default representado por el string "" vacío. Esto me define una topología que rabbit resuelve automáticamente en la que yo no tengo que especificar el binding de la routing key entre el exchange y la cola, porque el exchange por defecto es un exchange de tipo direct que mantiene el propio broker, y sobre el cual rabbit crea un binding implícito por cada cola que se declara, usando como binding key el mismo nombre de la cola.
Es decir, el binding existe igual solo que no lo escribo yo, lo escribe el broker en el momento en que declaro la cola. De hecho no podría declararlo a mano, porque rabbit rechaza cualquier bind de colas contra el exchange por defecto.

Por eso al publicar tengo que pasar como routing key el nombre exacto de la cola, no porque publish reciba una cola, sino porque la regla de ruteo de ese exchange compara la routing key del mensaje contra el binding key de cada binding, y en
este exchange ese binding key es siempre el nombre de la cola. Si pasara una routing key que no coincide con ninguna cola existente, el mensaje se descartaría silenciosamente. Lo cual entiendo es la diferencia concreta con el middleware de exchange, donde el binding sí lo tengo que declarar yo con QueueBind.

Como los consumidores son procesos que realizan trabajo, el procesamiento de los mensajes, siempre que el broker de rabbit este mandando un delivery, no hay forma de salir del loop que es bloqueante de ese proceso.
Para poder manejar eso de forma externa sin cerrar ni el canal ni la conexión, creo un nombre arbitrario para nombrar a ese consumidor y poder detener el consumo en su respectivo método de middleware. Es por eso que tuve que agregar esa variable de consumidor, para poder cancelar esa operación y manejar la idempotencia y un llamado antes de que haya comenzado a consumir algo siquiera.

### Decisiones

Creo una cola persistente, pero no creo mensajes persistentes. Es decir, si hubiera una caída en el broker (servidor de rabbit), cuando vuelva de ese restart, mi cola seguiría estando viva, pero no así los mensajes que hayan llegado a la cola porque no estoy cambiando el delivery mode de los mensajes a persistent que son por defecto transient. Al estar implementando un middleware genéricamente, me parece que esa decisión depende del tipo de sistema que esté usando el middleware y que tiene que haber una cierta intención.

Por ejemplo, una lectura de temperatura es una observación de estado dado que si se pierde, un segundo después llega otra que la reemplaza. En cambio, un mensaje que representa trabajo pendiente no lo regenera nadie: si el sistema encola tareas del tipo "generar el reporte del cliente 1234" y ese mensaje se pierde en un restart del broker, el reporte no se genera nunca y no se entera ni el productor ni el consumidor, porque para el productor el publish ya había retornado sin error. La pérdida sería silenciosa y no habría nada que la corrija.

No cierro el canal de forma explícita, que es una abstracción de rabbit por sobre la conexión, porque cuando llamo a Close(), la documentación me indica que cierra todo lo asociado a la conexión.

Si bien no tengo forma de saber cuanto trabajo demanda cada mensaje, siguiendo la documentación de rabbit me parece prudente modificar el límite de mensajes que el broker envía a los consumidores para que en vez de que se despachen de forma pareja por cantidad de mensajes ocurra por trabajo.

Por ejemplo, si tuviera 10 mensajes que pueden ser alternadamente pesados o livianos (tardando más segs respectivamente) repartidos entre dos consumidores, uno puede tardar sustancialmente más que el otro que estaría esperando por mensajes. Aunque el otro consumidor este "libre" no podría hacer nada porque esos mensajes ya estaría unacked asignados al primer consumidor y un mensaje tiene un único dueño a la vez.

Eligo prefetech de 1 e intercalo. Sobre el mismo ejemplo anterior, los pesados se reparten ahora entre ambos consumidores.

## Exchange

El giro en esta implementación fue entender que el exchange funciona solo como router de los mensajes que envía el productor.
Es decir, dado un mensaje y una routing key, consulta los bindings asociados para saber a qué cola de destino debe envía ese mensaje.

Basicamente, el bindeo está dado por los mensajes llegando al exchange E con una key K que van a una cola Q.

### Decisiones

Si al momento de mandar mensajes, todavía no hay un binding de la routing key a la cola, los mensajes simplemente no llegan (porque el exchange no persiste nada). No estoy manejando este escenario.

Elegir un exchange direct define únicamente qué mensajes entran a una cola (la routing key del mensaje tiene que coincidir exactamente con la binding key), pero no define a cuántos consumidores les llega. Si todos los consumidores comparten una misma cola con nombre bindeada a K, esa cola reparte round-robin y tengo competing consumers, o sea, un mensaje procesado por un único consumidor. Si en cambio cada consumidor declara su propia cola anónima bindeada a K, el exchange copia el mensaje en cada una de esas colas y todos reciben su propia copia.

Es también lo que me permite ocultar el detalle de la cola, porque la interfaz del middleware solo recibe el exchange y las routing keys y no hay ningún nombre de cola que exponer, así que dejo que el broker genere uno.

Ato la cola a la conexión para que nadie más pueda utilizarla y que le broker pueda eliminarla cuando la conexión se haya cerrado.

Como la cola vive lo que vive la conexión, tampoco tiene sentido que sea durable. Sí dejé el exchange como durable, porque a diferencia de las colas, el exchange es la pieza compartida de la topología y quiero que siga existiendo aunque en ese momento no haya nadie conectado.

Cambié en ambas implementaciones el flag para que no se re-encolen mensajes fallidos porque entro en un loop infinito, estoy eligiendo perder literalmente el mensaje, no implementé un mecanismo para recuperarme de esa condición.